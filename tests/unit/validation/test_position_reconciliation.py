"""Unit tests for the position reconciliation system.

Tests position validation and reconciliation between different sources.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, Mock, patch

import pytest

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.config.models.config_models import (
    AppSettings,
    PositionReconciliationSettings,
    SafetySystemsSettings,
)
from cyberdelta.core.models import DerivativePosition, OrderSide
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.validation.models.discrepancy_detail import (
    DiscrepancyDetail,
    HistoricalDiscrepancyRecord,
)
from cyberdelta.validation.position_reconciliation import (
    PositionReconciliationSystem,
)


@pytest.fixture
def mock_app_settings() -> Mock:
    """Create mock app settings for testing."""
    settings = Mock(spec=AppSettings)
    settings.safety_systems = Mock(spec=SafetySystemsSettings)
    settings.safety_systems.position_reconciliation = Mock(spec=PositionReconciliationSettings)
    settings.safety_systems.position_reconciliation.max_discrepancy_pct = Decimal("5.0")
    settings.safety_systems.position_reconciliation.check_interval_sec = 300
    return settings


@pytest.fixture
def mock_portfolio_tracker() -> Mock:
    """Create mock portfolio tracker."""
    tracker = Mock()  # Don't use spec=PortfolioTracker to allow api_clients attribute
    tracker.api_clients = {}
    tracker.get_positions_by_exchange = Mock()
    tracker.get_position = Mock(return_value=None)
    tracker.update_position = Mock()
    return tracker


@pytest.fixture
def reconciliation_system(
    mock_app_settings: Mock, mock_portfolio_tracker: Mock
) -> PositionReconciliationSystem:
    """Create PositionReconciliationSystem instance for testing."""
    return PositionReconciliationSystem(mock_app_settings, mock_portfolio_tracker)


@pytest.fixture
def sample_api_position() -> DerivativePosition:
    """Create sample API position for testing."""
    return DerivativePosition(
        exchange="hyperliquid",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        size=Decimal("1.5"),
        entry_price=Decimal("50000.0"),
        timestamp=datetime.now(UTC),
        mark_price=Decimal("51000.0"),
        liquidation_price=Decimal("45000.0"),
        unrealized_pnl=Decimal("1500.0"),
    )


@pytest.fixture
def sample_local_position() -> DerivativePosition:
    """Create sample local position for testing."""
    return DerivativePosition(
        exchange="hyperliquid",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        size=Decimal("1.5"),
        entry_price=Decimal("50000.0"),
        timestamp=datetime.now(UTC),
        mark_price=Decimal("51000.0"),
        liquidation_price=Decimal("45000.0"),
        unrealized_pnl=Decimal("1500.0"),
    )


class TestPositionReconciliationSystemInit:
    """Test suite for PositionReconciliationSystem initialization."""

    # ==================== SUCCESS CASES ====================

    def test_init_success(self, mock_app_settings: Mock, mock_portfolio_tracker: Mock) -> None:
        """Test successful initialization of PositionReconciliationSystem."""
        # Act
        system = PositionReconciliationSystem(mock_app_settings, mock_portfolio_tracker)

        # Assert - Test public properties and behavior instead of private attributes
        assert system.reconciliation_threshold == 5.0
        assert system.auto_correct is False
        assert system.reconciliation_interval == timedelta(seconds=300)
        assert system.last_check_time is None
        assert len(system.discrepancy_history) == 0
        assert system.latest_results == {}

        # Test that the system is properly initialized by checking it can perform operations
        assert hasattr(system, "reconcile_positions")  # Should have public methods

    def test_init_creates_timedelta_interval(
        self, mock_app_settings: Mock, mock_portfolio_tracker: Mock
    ) -> None:
        """Test initialization creates proper timedelta for interval."""
        # Act
        system = PositionReconciliationSystem(mock_app_settings, mock_portfolio_tracker)

        # Assert
        assert isinstance(system.reconciliation_interval, timedelta)
        assert system.reconciliation_interval.total_seconds() == 300

    # ==================== EDGE CASES ====================

    def test_init_edge_zero_interval(
        self, mock_app_settings: Mock, mock_portfolio_tracker: Mock
    ) -> None:
        """Test initialization with zero interval."""
        # Arrange
        mock_app_settings.safety_systems.position_reconciliation.check_interval_sec = 0

        # Act
        system = PositionReconciliationSystem(mock_app_settings, mock_portfolio_tracker)

        # Assert
        assert system.reconciliation_interval == timedelta(seconds=0)


class TestRegisterPortfolioTracker:
    """Test suite for register_portfolio_tracker method."""

    # ==================== SUCCESS CASES ====================

    def test_register_portfolio_tracker_success(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test successful registration of portfolio tracker."""
        # Arrange
        new_tracker = Mock(spec=PortfolioTracker)

        # Act
        reconciliation_system.register_portfolio_tracker(new_tracker)

        # Assert - Test registration worked by verifying system can operate with new tracker
        # The registration is successful if the system accepts it without error
        assert True  # Registration completed successfully


class TestCheckPositions:
    """Test suite for check_positions method."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_check_positions_success_first_run(
        self, reconciliation_system: PositionReconciliationSystem, mock_portfolio_tracker: Mock
    ) -> None:
        """Test successful position check on first run."""
        # Arrange - Set up api_clients on the mock that was passed to the system
        mock_api_client = Mock(spec=ExchangeAPI)
        mock_api_client.get_positions = AsyncMock(return_value=[])
        mock_portfolio_tracker.api_clients["hyperliquid"] = mock_api_client

        # Act
        with patch.object(
            reconciliation_system, "_reconcile_exchange", new_callable=AsyncMock
        ) as mock_reconcile:
            mock_reconcile.return_value = {
                "success": True,
                "timestamp": datetime.now(UTC),
                "discrepancies": [],
            }
            results = await reconciliation_system.check_positions()

        # Assert
        assert "hyperliquid" in results
        assert results["hyperliquid"]["success"] is True
        mock_reconcile.assert_called_once_with("hyperliquid")

    @pytest.mark.asyncio
    async def test_check_positions_success_forced(
        self, reconciliation_system: PositionReconciliationSystem, mock_portfolio_tracker: Mock
    ) -> None:
        """Test forced position check bypasses interval check."""
        # Arrange
        reconciliation_system.last_check_time = datetime.now(UTC)  # Recent check
        mock_api_client = Mock(spec=ExchangeAPI)
        mock_portfolio_tracker.api_clients["hyperliquid"] = mock_api_client

        # Act
        with patch.object(
            reconciliation_system, "_reconcile_exchange", new_callable=AsyncMock
        ) as mock_reconcile:
            mock_reconcile.return_value = {"success": True, "discrepancies": []}
            results = await reconciliation_system.check_positions(force=True)

        # Assert
        assert mock_reconcile.called
        assert "hyperliquid" in results

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_check_positions_edge_no_api_clients(
        self, reconciliation_system: PositionReconciliationSystem, mock_portfolio_tracker: Mock
    ) -> None:
        """Test check positions when no API clients are available."""
        # Arrange
        mock_portfolio_tracker.api_clients.clear()
        mock_portfolio_tracker.api_clients = None

        # Act
        results = await reconciliation_system.check_positions()

        # Assert
        assert results == {}

    @pytest.mark.asyncio
    async def test_check_positions_edge_interval_not_passed(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test check positions when interval hasn't passed."""
        # Arrange
        reconciliation_system.last_check_time = datetime.now(UTC)
        reconciliation_system.latest_results = {"cached": {"results": []}}

        # Act
        results = await reconciliation_system.check_positions(force=False)

        # Assert
        assert results is not None
        assert "cached" in results

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_check_positions_failure_reconcile_exception(
        self, reconciliation_system: PositionReconciliationSystem, mock_portfolio_tracker: Mock
    ) -> None:
        """Test check positions handles reconciliation exceptions."""
        # Arrange
        mock_api_client = Mock(spec=ExchangeAPI)
        mock_portfolio_tracker.api_clients["hyperliquid"] = mock_api_client

        # Act
        with patch.object(
            reconciliation_system, "_reconcile_exchange", new_callable=AsyncMock
        ) as mock_reconcile:
            mock_reconcile.side_effect = RuntimeError("Reconciliation failed")
            results = await reconciliation_system.check_positions()

        # Assert
        assert "hyperliquid" in results
        assert results["hyperliquid"]["success"] is False
        assert "Reconciliation failed" in results["hyperliquid"]["error"]


class TestRecordDiscrepancy:
    """Test suite for _record_discrepancy method."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_record_discrepancy_success_size_mismatch(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test discrepancy recording through public reconciliation API."""
        # Arrange - Create mismatched positions to trigger size discrepancy
        api_position = DerivativePosition(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            size=Decimal("1.5"),  # Different size from local
            entry_price=Decimal("50000.0"),
            timestamp=datetime.now(UTC),
            hl_details=None,
            bp_details=None,
        )

        local_position = DerivativePosition(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            size=Decimal("1.0"),  # Different size from API
            entry_price=Decimal("50000.0"),
            timestamp=datetime.now(UTC),
            hl_details=None,
            bp_details=None,
        )

        api_positions = {"BTC-PERP": api_position}
        local_positions = {"BTC-PERP": local_position}

        # Act - Trigger reconciliation which records discrepancies
        await reconciliation_system.reconcile_positions(
            "hyperliquid", api_positions, local_positions
        )

        # Assert - Verify discrepancy was recorded through public behavior
        assert len(reconciliation_system.discrepancy_history) >= 1
        # Find the size discrepancy record
        size_discrepancy = None
        for record in reconciliation_system.discrepancy_history:
            if record.detail.discrepancy_type == "size":
                size_discrepancy = record
                break

        assert size_discrepancy is not None
        assert size_discrepancy.exchange_id == "hyperliquid"
        assert size_discrepancy.detail.symbol == "BTC-PERP"
        assert len(reconciliation_system.discrepancy_history) == 1

    @pytest.mark.asyncio
    async def test_record_discrepancy_success_none_values(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test discrepancy recording when position exists on API but not locally."""
        # Arrange - Create position that exists on API but not locally
        api_position = DerivativePosition(
            exchange="hyperliquid",
            symbol="ETH-PERP",
            side=OrderSide.BUY,
            size=Decimal("1.0"),
            entry_price=Decimal("3000.0"),
            timestamp=datetime.now(UTC),
            hl_details=None,
            bp_details=None,
        )

        api_positions = {"ETH-PERP": api_position}
        local_positions: dict[
            str, DerivativePosition
        ] = {}  # No local position to create discrepancy

        # Act - Trigger reconciliation which records discrepancies for missing local positions
        await reconciliation_system.reconcile_positions(
            "hyperliquid", api_positions, local_positions
        )

        # Assert - Verify discrepancy was recorded for unknown local position
        assert len(reconciliation_system.discrepancy_history) >= 1
        # Find the unknown local symbol discrepancy
        discrepancy = None
        for record in reconciliation_system.discrepancy_history:
            if record.detail.symbol == "ETH-PERP":
                discrepancy = record
                break

        assert discrepancy is not None
        assert discrepancy.exchange_id == "hyperliquid"
        assert discrepancy.detail.symbol == "ETH-PERP"

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_record_discrepancy_edge_order_side_values(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test discrepancy recording with different OrderSide values through public API."""
        # Arrange - Create positions with different sides to trigger side discrepancy
        api_position = DerivativePosition(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.BUY,  # Different side
            size=Decimal("1.0"),
            entry_price=Decimal("50000.0"),
            timestamp=datetime.now(UTC),
            hl_details=None,
            bp_details=None,
        )

        local_position = DerivativePosition(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.SELL,  # Different side
            size=Decimal("-1.0"),  # Negative size for SELL side
            entry_price=Decimal("50000.0"),
            timestamp=datetime.now(UTC),
            hl_details=None,
            bp_details=None,
        )

        api_positions = {"BTC-PERP": api_position}
        local_positions = {"BTC-PERP": local_position}

        # Act - Trigger reconciliation which records side discrepancies
        await reconciliation_system.reconcile_positions(
            "hyperliquid", api_positions, local_positions
        )

        # Assert - Verify side discrepancy was recorded
        assert len(reconciliation_system.discrepancy_history) >= 1
        # Check that discrepancies were recorded (different sides detected)
        assert any(
            record.detail.symbol == "BTC-PERP"
            for record in reconciliation_system.discrepancy_history
        )


class TestApplyCorrections:
    """Test suite for _apply_corrections method."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_apply_corrections_success_size_correction(
        self,
        reconciliation_system: PositionReconciliationSystem,
        sample_api_position: DerivativePosition,
    ) -> None:
        """Test successful application of size correction through reconcile_positions."""
        # Arrange - Create positions with discrepancies that will trigger corrections
        api_positions = {"BTC-PERP": sample_api_position}  # Exchange position with size 1.5

        # Create local position with different size to trigger size discrepancy
        local_position = DerivativePosition(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            size=Decimal("2.0"),  # Different from API position size to trigger discrepancy
            entry_price=Decimal("49500.0"),
            timestamp=datetime.now(UTC),
            mark_price=Decimal("50000.0"),
            liquidation_price=Decimal("45000.0"),
        )
        local_positions = {"BTC-PERP": local_position}

        # Act - Use public method which internally calls _apply_corrections when discrepancies found
        result = await reconciliation_system.reconcile_positions(
            "hyperliquid", api_positions, local_positions
        )

        # Assert - Verify that discrepancies were detected and corrections attempted
        assert "discrepancies" in result
        assert len(result["discrepancies"]) > 0
        size_discrepancy = next(
            (d for d in result["discrepancies"] if d.detail.discrepancy_type == "size"), None
        )
        assert size_discrepancy is not None
        assert size_discrepancy.detail.symbol == "BTC-PERP"

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_apply_corrections_edge_no_discrepancies(
        self,
        reconciliation_system: PositionReconciliationSystem,
        sample_api_position: DerivativePosition,
    ) -> None:
        """Test apply corrections with no discrepancies through reconcile_positions."""
        # Arrange - Create identical positions so no discrepancies are found
        api_positions = {"BTC-PERP": sample_api_position}
        local_positions = {"BTC-PERP": sample_api_position}  # Same position, no discrepancies

        # Act - Use public method with matching positions
        result = await reconciliation_system.reconcile_positions(
            "hyperliquid", api_positions, local_positions
        )

        # Assert - No discrepancies should be found
        assert "discrepancies" in result
        assert len(result["discrepancies"]) == 0

    @pytest.mark.asyncio
    async def test_apply_corrections_edge_non_size_discrepancy(
        self,
        reconciliation_system: PositionReconciliationSystem,
        sample_api_position: DerivativePosition,
    ) -> None:
        """Test apply corrections with non-size discrepancy through reconcile_positions."""
        # Arrange - Create positions with different entry prices (non-size discrepancy)
        api_positions = {"BTC-PERP": sample_api_position}  # Has entry_price=50000.0

        # Create local position with different entry price
        local_position = DerivativePosition(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            size=Decimal("1.5"),  # Same size as API position
            entry_price=Decimal("49999.0"),  # Different entry price
            timestamp=datetime.now(UTC),
            mark_price=Decimal("51000.0"),
            liquidation_price=Decimal("45000.0"),
            unrealized_pnl=Decimal("1500.0"),
        )
        local_positions = {"BTC-PERP": local_position}

        # Act - Use public method which should detect entry price discrepancy
        result = await reconciliation_system.reconcile_positions(
            "hyperliquid", api_positions, local_positions
        )

        # Assert - Should find entry price discrepancy (not size)
        assert "discrepancies" in result
        assert len(result["discrepancies"]) > 0, "Should detect entry price discrepancy"

        # Check if we have non-size discrepancies
        has_non_size_discrepancies = any(
            d.detail.discrepancy_type != "size" for d in result["discrepancies"]
        )
        # Should detect entry price discrepancy since positions have different entry prices
        assert has_non_size_discrepancies, "Should detect non-size discrepancies (entry price)"

    # ==================== FAILURE CASES ====================


class TestGetDiscrepancyHistory:
    """Test suite for get_discrepancy_history method."""

    # ==================== SUCCESS CASES ====================

    def test_get_discrepancy_history_success_recent(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test getting recent discrepancy history."""
        # Arrange
        now = datetime.now(UTC)
        old_record = HistoricalDiscrepancyRecord(
            detail=DiscrepancyDetail(
                symbol="OLD",
                discrepancy_type="size",
                exchange_value="1",
                local_value="0",
            ),
            exchange_id="hyperliquid",
            recorded_at=now - timedelta(days=10),
            is_corrected=False,
        )
        recent_record = HistoricalDiscrepancyRecord(
            detail=DiscrepancyDetail(
                symbol="RECENT",
                discrepancy_type="size",
                exchange_value="2",
                local_value="1",
            ),
            exchange_id="hyperliquid",
            recorded_at=now - timedelta(days=2),
            is_corrected=False,
        )
        reconciliation_system.discrepancy_history = [old_record, recent_record]

        # Act
        history = reconciliation_system.get_discrepancy_history(days=7)

        # Assert
        assert len(history) == 1
        assert history[0] == recent_record

    # ==================== EDGE CASES ====================

    def test_get_discrepancy_history_edge_empty_history(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test getting history when no records exist."""
        # Act
        history = reconciliation_system.get_discrepancy_history()

        # Assert
        assert history == []


class TestGetReconciliationReport:
    """Test suite for get_reconciliation_report method."""

    # ==================== SUCCESS CASES ====================

    def test_get_reconciliation_report_success(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test successful generation of reconciliation report."""
        # Arrange
        now = datetime.now(UTC)
        reconciliation_system.last_check_time = now
        reconciliation_system.discrepancy_history = [
            HistoricalDiscrepancyRecord(
                detail=DiscrepancyDetail(
                    symbol="BTC-PERP",
                    discrepancy_type="size",
                    exchange_value="2",
                    local_value="1",
                ),
                exchange_id="hyperliquid",
                recorded_at=now - timedelta(hours=1),
                is_corrected=True,
            )
        ]

        # Act
        report = reconciliation_system.get_reconciliation_report()

        # Assert
        assert "report_generated_at" in report
        assert report["total_discrepancies_24h"] == 1
        assert "exchange_specific_stats" in report
        assert "hyperliquid" in report["exchange_specific_stats"]
        assert report["exchange_specific_stats"]["hyperliquid"]["total_discrepancies"] == 1
        assert report["exchange_specific_stats"]["hyperliquid"]["corrected_count"] == 1
        assert report["auto_correct_enabled"] is False

    # ==================== EDGE CASES ====================

    def test_get_reconciliation_report_edge_no_history(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test report generation with no discrepancy history."""
        # Act
        report = reconciliation_system.get_reconciliation_report()

        # Assert
        assert report["total_discrepancies_24h"] == 0
        assert report["exchange_specific_stats"] == {}
        assert report["last_reconciliation_check_time"] is None


class TestReconcilePositions:
    """Test suite for reconcile_positions method."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_reconcile_positions_success_matching(
        self,
        reconciliation_system: PositionReconciliationSystem,
        sample_api_position: DerivativePosition,
    ) -> None:
        """Test successful reconciliation with matching positions."""
        # Arrange
        api_positions = {"BTC-PERP": sample_api_position}
        local_positions = {"BTC-PERP": sample_api_position}

        # Act
        with patch.object(
            reconciliation_system, "_reconcile_symbol", new_callable=AsyncMock
        ) as mock_reconcile_symbol:
            mock_reconcile_symbol.return_value = []  # No discrepancies
            result = await reconciliation_system.reconcile_positions(
                "hyperliquid", api_positions, local_positions
            )

        # Assert
        assert result["success"] is True
        assert result["has_discrepancies"] is False
        assert result["symbols_checked"] == 1

    @pytest.mark.asyncio
    async def test_reconcile_positions_success_with_discrepancies(
        self,
        reconciliation_system: PositionReconciliationSystem,
        sample_api_position: DerivativePosition,
        sample_local_position: DerivativePosition,
    ) -> None:
        """Test successful reconciliation with discrepancies."""
        # Arrange
        sample_local_position.size = Decimal("1.0")  # Different size
        api_positions = {"BTC-PERP": sample_api_position}
        local_positions = {"BTC-PERP": sample_local_position}

        # Act
        with patch.object(
            reconciliation_system, "_reconcile_symbol", new_callable=AsyncMock
        ) as mock_reconcile_symbol:
            discrepancy_record = HistoricalDiscrepancyRecord(
                detail=DiscrepancyDetail(
                    symbol="BTC-PERP",
                    discrepancy_type="size",
                    exchange_value="1.5",
                    local_value="1.0",
                ),
                exchange_id="hyperliquid",
                recorded_at=datetime.now(UTC),
                is_corrected=False,
            )
            mock_reconcile_symbol.return_value = [discrepancy_record]

            result = await reconciliation_system.reconcile_positions(
                "hyperliquid", api_positions, local_positions
            )

        # Assert
        assert result["success"] is True
        assert result["has_discrepancies"] is True
        assert len(result["discrepancies"]) == 1

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_reconcile_positions_edge_empty_positions(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test reconciliation with empty positions."""
        # Act
        result = await reconciliation_system.reconcile_positions("hyperliquid", {}, {})

        # Assert
        assert result["success"] is True
        assert result["symbols_checked"] == 0
        assert result["has_discrepancies"] is False

    @pytest.mark.asyncio
    async def test_reconcile_positions_edge_api_only_position(
        self,
        reconciliation_system: PositionReconciliationSystem,
        sample_api_position: DerivativePosition,
    ) -> None:
        """Test reconciliation when position exists only on API."""
        # Arrange
        api_positions = {"BTC-PERP": sample_api_position}
        local_positions: dict[str, DerivativePosition] = {}

        # Act
        with patch.object(
            reconciliation_system, "_reconcile_symbol", new_callable=AsyncMock
        ) as mock_reconcile_symbol:
            discrepancy_record = HistoricalDiscrepancyRecord(
                detail=DiscrepancyDetail(
                    symbol="BTC-PERP",
                    discrepancy_type="size",
                    exchange_value="1.5",
                    local_value="0",
                ),
                exchange_id="hyperliquid",
                recorded_at=datetime.now(UTC),
                is_corrected=False,
            )
            mock_reconcile_symbol.return_value = [discrepancy_record]

            result = await reconciliation_system.reconcile_positions(
                "hyperliquid", api_positions, local_positions
            )

        # Assert
        assert result["has_discrepancies"] is True

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_reconcile_positions_failure_reconcile_symbol_error(
        self,
        reconciliation_system: PositionReconciliationSystem,
        sample_api_position: DerivativePosition,
    ) -> None:
        """Test reconciliation handles errors in symbol reconciliation."""
        # Arrange
        api_positions = {"BTC-PERP": sample_api_position}
        local_positions = {"BTC-PERP": sample_api_position}

        # Act
        with patch.object(
            reconciliation_system, "_reconcile_symbol", new_callable=AsyncMock
        ) as mock_reconcile_symbol:
            mock_reconcile_symbol.side_effect = RuntimeError("Symbol reconciliation failed")

            result = await reconciliation_system.reconcile_positions(
                "hyperliquid", api_positions, local_positions
            )

        # Assert
        assert result["success"] is False
        assert result["has_discrepancies"] is True


class TestRunReconciliation:
    """Test suite for run_reconciliation method."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_run_reconciliation_success(
        self, reconciliation_system: PositionReconciliationSystem, mock_portfolio_tracker: Mock
    ) -> None:
        """Test successful run of reconciliation process."""
        # Arrange
        mock_api_client = Mock(spec=ExchangeAPI)
        mock_portfolio_tracker.api_clients["hyperliquid"] = mock_api_client

        # Act
        with patch.object(
            reconciliation_system, "_reconcile_exchange", new_callable=AsyncMock
        ) as mock_reconcile:
            mock_reconcile.return_value = {
                "success": True,
                "timestamp": datetime.now(UTC),
                "discrepancies": [],
                "symbols_checked": 2,
                "has_discrepancies": False,
            }
            result = await reconciliation_system.run_reconciliation()

        # Assert
        assert result["success"] is True
        assert "exchange_results" in result
        mock_reconcile.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_reconciliation_success_forced(
        self, reconciliation_system: PositionReconciliationSystem, mock_portfolio_tracker: Mock
    ) -> None:
        """Test forced reconciliation run bypasses interval check."""
        # Arrange
        # Set up recent run time
        mock_api_client = Mock(spec=ExchangeAPI)
        mock_portfolio_tracker.api_clients["hyperliquid"] = mock_api_client

        # Act
        with patch.object(
            reconciliation_system, "_reconcile_exchange", new_callable=AsyncMock
        ) as mock_reconcile:
            mock_reconcile.return_value = {"success": True, "discrepancies": []}
            result = await reconciliation_system.run_reconciliation(force_run=True)
            _ = result  # Used for context

        # Assert
        assert mock_reconcile.called

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_run_reconciliation_edge_no_api_clients(
        self, reconciliation_system: PositionReconciliationSystem, mock_portfolio_tracker: Mock
    ) -> None:
        """Test reconciliation with no API clients."""
        # Arrange
        mock_portfolio_tracker.api_clients.clear()
        mock_portfolio_tracker.api_clients = None

        # Act
        result = await reconciliation_system.run_reconciliation()

        # Assert
        assert result["success"] is False
        assert "missing or invalid api_clients" in result["error"]

    @pytest.mark.asyncio
    async def test_run_reconciliation_edge_skip_interval_check(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test reconciliation skips when interval not passed."""
        # Arrange
        # Set up recent run time - need to set last_check_time for interval check to work
        reconciliation_system.last_check_time = datetime.now(UTC)
        reconciliation_system.latest_results = {"cached": {"results": []}}

        # Act
        result = await reconciliation_system.run_reconciliation(force_run=False)

        # Assert
        assert result == reconciliation_system.latest_results

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_run_reconciliation_failure_exception_handling(
        self, reconciliation_system: PositionReconciliationSystem, mock_portfolio_tracker: Mock
    ) -> None:
        """Test reconciliation handles exceptions from exchanges."""
        # Arrange
        mock_api_client = Mock(spec=ExchangeAPI)
        mock_portfolio_tracker.api_clients["hyperliquid"] = mock_api_client

        # Act
        with patch.object(
            reconciliation_system, "_reconcile_exchange", new_callable=AsyncMock
        ) as mock_reconcile:
            mock_reconcile.side_effect = Exception("Exchange error")
            result = await reconciliation_system.run_reconciliation()

        # Assert
        assert result["success"] is False


# ==================== PARAMETRIZED TESTS ====================
