import pytest
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import MagicMock
import asyncio

from cyberdelta.core.execution_handler import ExecutionHandler, ExecutionStatus
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.core.models import (
    OrderSide,
    Balance,
    Position,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem, BreakerState
from tests.integration.conftest import create_mock_ticker, basic_opportunity
from tests.integration.mocks.mock_exchange import MockExchangeAPI

# Fixtures will be reused from tests/integration/conftest.py


@pytest.mark.asyncio
async def test_circuit_breaker_global_halts_execution(
    mock_config,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    real_portfolio_tracker: PortfolioTracker,
    data_handler,
    signal_generator,
    risk_manager,
    execution_handler: ExecutionHandler,
    circuit_breaker_system: CircuitBreakerSystem,
):
    """Tests that a globally open circuit breaker prevents new executions."""
    # 1. Setup - Basic state, no initial errors
    mock_bp_api.reset()
    mock_hl_api.reset()
    real_portfolio_tracker.reset()
    # Resetting the system might involve clearing internal states if implemented
    # For now, assume creating a new system resets state, or add a reset method if needed.
    # circuit_breaker_system.reset() # Add if a reset method exists

    mock_bp_api.set_mock_balance(Balance(asset="USDC", total=Decimal("10000"), free=Decimal("10000")))
    mock_hl_api.set_mock_balance(Balance(asset="USD", total=Decimal("10000"), free=Decimal("10000")))
    await real_portfolio_tracker.initialize()
    mock_bp_api.set_mock_ticker(create_mock_ticker("BTC-PERP", 30000, 30001, 30000.5))
    mock_hl_api.set_mock_ticker(create_mock_ticker("BTC-PERP", 30010, 30011, 30010.5))

    # 2. Trigger Global Circuit Breaker
    # Simulate enough API errors to trip the global breaker (using APIErrorBreaker logic)
    # Assuming APIErrorBreaker is configured and used by the system
    # Access config via the system's config object
    failure_threshold = circuit_breaker_system.config.get("validation.circuit_breaker.global.failure_threshold", 3)

    # Use the system's method to record errors
    dummy_error_msg = "Simulated API Error"
    # Need to associate error with an exchange, even for global check?
    # The system's can_execute checks exchange-specific breakers.
    # Let's record errors against a specific exchange to trigger its APIErrorBreaker.
    exchange_for_global = "mock_bp" # Choose one exchange for simulation

    for _ in range(failure_threshold):
        circuit_breaker_system.record_api_error(exchange_for_global, dummy_error_msg)
        # Need a small delay if windowing is involved
        await asyncio.sleep(0.01)

    can_exec, reason = circuit_breaker_system.can_execute(exchange="global") # Check global state
    assert can_exec is False, f"Global breaker should be open, reason: {reason}"

    # 3. Attempt Execution
    sized_opp = basic_sized_opportunity()  # Use helper from conftest

    execution_result = await execution_handler.execute_opportunity(sized_opp)

    # 4. Verify Rejection
    assert execution_result.status == ExecutionStatus.REJECTED, (
        f"Expected REJECTED status, got {execution_result.status}"
    )
    assert execution_result.error_message is not None
    assert "Global circuit breaker is open" in execution_result.error_message

    # Verify no orders were placed (mocks shouldn't have been called)
    assert not mock_bp_api.get_all_orders()
    assert not mock_hl_api.get_all_orders()


@pytest.mark.asyncio
async def test_circuit_breaker_exchange_halts_execution(
    mock_config,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    real_portfolio_tracker: PortfolioTracker,
    data_handler,
    signal_generator,
    risk_manager,
    execution_handler: ExecutionHandler,
    circuit_breaker_system: CircuitBreakerSystem,
):
    """Tests that an open exchange circuit breaker prevents executions involving that exchange."""
    # 1. Setup - Basic state, no initial errors
    mock_bp_api.reset()
    mock_hl_api.reset()
    real_portfolio_tracker.reset()
    # Resetting the system might involve clearing internal states if implemented
    # circuit_breaker_system.reset() # Add if a reset method exists

    mock_bp_api.set_mock_balance(Balance(asset="USDC", total=Decimal("10000"), free=Decimal("10000")))
    mock_hl_api.set_mock_balance(Balance(asset="USD", total=Decimal("10000"), free=Decimal("10000")))
    await real_portfolio_tracker.initialize()
    mock_bp_api.set_mock_ticker(create_mock_ticker("BTC-PERP", 30000, 30001, 30000.5))
    mock_hl_api.set_mock_ticker(create_mock_ticker("BTC-PERP", 30010, 30011, 30010.5))

    # 2. Trigger Exchange Circuit Breaker (e.g., for mock_bp)
    bp_exchange_id = "mock_bp"
    # Use the system's method to record errors for the specific exchange
    # Get threshold from the system's config object
    config_path_prefix = f"validation.circuit_breaker.exchanges.{bp_exchange_id}.api_errors"
    api_breaker_enabled = circuit_breaker_system.config.get(f"{config_path_prefix}.enabled", True)
    failure_threshold = circuit_breaker_system.config.get(f"{config_path_prefix}.threshold", 2) if api_breaker_enabled else 2
    dummy_error_msg = "Simulated API Error for mock_bp"

    for _ in range(failure_threshold):
        circuit_breaker_system.record_api_error(bp_exchange_id, dummy_error_msg)
        await asyncio.sleep(0.01) # Sleep if windowing applies

    # Verify the specific exchange breaker is open using can_execute
    can_exec_bp, reason_bp = circuit_breaker_system.can_execute(exchange=bp_exchange_id)
    assert can_exec_bp is False, f"{bp_exchange_id} breaker should be open, reason: {reason_bp}"

    # Verify global is still closed
    can_exec_global, reason_global = circuit_breaker_system.can_execute(exchange="global")
    assert can_exec_global is True, f"Global breaker should remain closed, reason: {reason_global}"

    # 3. Attempt Execution involving the tripped exchange (mock_bp)
    sized_opp = basic_sized_opportunity()  # Default uses mock_bp and mock_hl

    execution_result = await execution_handler.execute_opportunity(sized_opp)

    # 4. Verify Rejection
    assert execution_result.status == ExecutionStatus.REJECTED, (
        f"Expected REJECTED status, got {execution_result.status}"
    )
    assert execution_result.error_message is not None
    assert (
        f"Circuit breaker open for {bp_exchange_id}" in execution_result.error_message
    )

    # Verify no orders were placed
    assert not mock_bp_api.get_all_orders()
    assert not mock_hl_api.get_all_orders()

    # 5. Attempt Execution NOT involving the tripped exchange (if possible)
    # This requires setting up a third mock exchange or modifying the opportunity
    # Skipping this part for simplicity for now.


@pytest.mark.asyncio
async def test_funding_rate_validator_reduces_size(
    mock_config,
    real_portfolio_tracker: PortfolioTracker,
    data_handler,
    risk_manager: RiskManager,
    funding_rate_validator: MagicMock,  # Inject the mock validator fixture
    basic_opportunity,  # Use the basic opportunity fixture
):
    """Tests that poor validation metrics reduce the calculated position size."""
    # 1. Setup: Ensure validator is attached to risk_manager
    # This should happen via fixtures in conftest.py based on RiskManager constructor
    assert risk_manager.funding_rate_validator is funding_rate_validator

    # Configure validator mock to return poor metrics (e.g., high RMSE/bias -> low factor)
    low_confidence_factor = 0.2  # Example: Only 20% confidence -> results in 0.2 factor
    funding_rate_validator.get_validation_metrics.return_value = {
        # Return metrics that would result in the low_confidence_factor
        # The exact calculation depends on _get_validation_metrics logic in RiskManager
        # Assuming simple logic for now, just mock the final factor calculation step indirectly
        # by having the mock return the desired *float* factor.
        # We can achieve this by mocking the _get_validation_metrics internal helper if needed,
        # but mocking the validator interface is cleaner.
        # Let's assume the validator itself provides a confidence score/factor directly for simplicity
        # If not, we'd mock rmse/bias values.
        "confidence_factor": low_confidence_factor  # Mocking a direct confidence factor
    }
    # Adjusting: Since _get_validation_metrics in RiskManager calculates the factor from RMSE/Bias,
    # we need to mock the return value of get_validation_metrics to be a dict with rmse/bias.
    # Let's mock values that will definitely trigger a reduction.
    high_rmse = risk_manager.max_acceptable_rmse * 2  # e.g., 0.1 if max is 0.05
    high_bias = risk_manager.max_acceptable_bias * 2  # e.g., 0.04 if max is 0.02
    funding_rate_validator.get_validation_metrics.return_value = {
        "rmse": high_rmse,
        "bias": high_bias,
    }
    min_factor = risk_manager.min_validation_factor  # e.g., 0.2

    # 2. Calculate size normally (as baseline)
    # Temporarily disable validator influence for baseline
    risk_manager.funding_rate_validator = None
    baseline_sized_opportunities = risk_manager.validate_opportunities(
        [basic_opportunity]
    )
    risk_manager.funding_rate_validator = funding_rate_validator  # Restore validator

    assert len(baseline_sized_opportunities) == 1, "Baseline sizing failed"
    baseline_size = baseline_sized_opportunities[
        0
    ].long_size  # Use long_size as representative
    assert baseline_size > 0

    # 3. Calculate size with validator active (expecting reduction)
    validated_sized_opportunities = risk_manager.validate_opportunities(
        [basic_opportunity]
    )

    assert len(validated_sized_opportunities) == 1, "Validated sizing failed"
    validated_size = validated_sized_opportunities[0].long_size

    # 4. Verify Size Reduction
    # The expected size should be baseline_size * min_factor (as RMSE/Bias were high)
    # Need to ensure min_factor itself is Decimal or properly converted in RiskManager
    expected_factor = Decimal(str(min_factor))
    expected_size = baseline_size * expected_factor

    # Use pytest.approx for Decimal comparison
    assert validated_size == pytest.approx(expected_size), (
        f"Expected size reduced to approx {expected_size} due to validation, got {validated_size}"
    )
    assert validated_size < baseline_size, (
        "Validated size should be smaller than baseline"
    )

    # Check that the validator method was called
    funding_rate_validator.get_validation_metrics.assert_called()


@pytest.mark.asyncio
async def test_position_reconciler_detects_discrepancy(
    mock_config,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    real_portfolio_tracker: PortfolioTracker,
    position_reconciler,
):
    """Tests that the PositionReconciliationSystem identifies discrepancies."""
    # 1. Setup - Place a known position via mock API update, tracker should be empty initially
    mock_bp_api.reset()
    real_portfolio_tracker.reset()

    exchange_id = "mock_bp"
    symbol = "BTC-PERP"
    position_id = f"{exchange_id}_{symbol}_testpos"
    mock_position = Position(
        id=position_id,
        symbol=symbol,
        size=Decimal("0.1"),  # Use Decimal
        entry_price=Decimal("30000"),  # Use Decimal
        mark_price=Decimal("30100"),  # Use Decimal
        side=OrderSide.BUY,
        status="OPEN",
    )

    # Manually set position in mock API, but NOT in tracker
    mock_bp_api._positions[symbol] = mock_position  # Simplified access for test

    # 2. Run Reconciliation
    # Assume reconciler uses portfolio_tracker.api_clients
    if exchange_id not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client(exchange_id, mock_bp_api)

    # Correct method name: check_positions()
    discrepancies_result = await position_reconciler.check_positions()
    discrepancies = discrepancies_result.get(exchange_id, {}).get('discrepancies', [])

    # 3. Verify Discrepancy Detection
    assert len(discrepancies) > 0, "Expected reconciler to find discrepancies"

    found_missing_in_tracker = False
    for disc in discrepancies:
        # Example check - structure depends on how reconciler reports
        if (
            disc.get("exchange") == exchange_id
            and disc.get("symbol") == symbol
            and disc.get("discrepancy_type") == "missing_in_tracker"
            and disc.get("exchange_size") == mock_position.size
        ):
            found_missing_in_tracker = True
            break

    assert found_missing_in_tracker, (
        "Did not find the expected 'missing_in_tracker' discrepancy"
    )

    # 4. Setup Reverse Scenario - Position in tracker, not on exchange
    real_portfolio_tracker.reset()
    mock_bp_api.reset()  # Clear position from mock API
    real_portfolio_tracker.update_position(exchange_id, mock_position)  # Add to real tracker

    # Ensure API client is registered on real tracker
    if exchange_id not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client(exchange_id, mock_bp_api)

    # Correct method name: check_positions()
    discrepancies_reverse_result = await position_reconciler.check_positions()
    discrepancies_reverse = discrepancies_reverse_result.get(exchange_id, {}).get('discrepancies', [])

    # 5. Verify Reverse Discrepancy Detection
    assert len(discrepancies_reverse) > 0, (
        "Expected reconciler to find discrepancies (reverse)"
    )
    found_missing_on_exchange = False
    for disc in discrepancies_reverse:
        if (
            disc.get("exchange") == exchange_id
            and disc.get("symbol") == symbol
            and disc.get("discrepancy_type") == "missing_on_exchange"
            and disc.get("tracker_size") == mock_position.size
        ):
            found_missing_on_exchange = True
            break

    assert found_missing_on_exchange, (
        "Did not find the expected 'missing_on_exchange' discrepancy"
    )
