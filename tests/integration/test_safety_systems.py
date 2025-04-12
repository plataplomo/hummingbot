import logging

# === FORCE ROOT LOGGER LEVEL ===
logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)  # ADD logger instance
# ============================

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from cyberdelta.core.execution_handler import (
    ExecutionHandler,
    ExecutionStatus,
    TradeExecution,
)  # Added TradeExecution
from cyberdelta.core.models import (
    ArbitrageOpportunity,
    Balance,
    OrderSide,
    Position,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem
from tests.integration.conftest import create_mock_ticker
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
    basic_opportunity: ArbitrageOpportunity,
):
    """Tests that a globally open circuit breaker prevents new executions."""
    # 1. Setup - Basic state, no initial errors
    mock_bp_api.reset()
    mock_hl_api.reset()
    real_portfolio_tracker.reset()
    # Explicitly reset breakers associated with the system
    circuit_breaker_system.reset_breaker("global_api_error")  # Reset global
    circuit_breaker_system.reset_exchange_breakers("mock_bp")  # Reset exchange specific
    circuit_breaker_system.reset_exchange_breakers("mock_hl")

    mock_bp_api.set_mock_balance(
        Balance(asset="USDC", total=Decimal("10000"), available=Decimal("10000"))
    )
    mock_hl_api.set_mock_balance(
        Balance(asset="USD", total=Decimal("10000"), available=Decimal("10000"))
    )
    await real_portfolio_tracker.initialize()
    ts = datetime.now(UTC)
    mock_bp_api.set_mock_ticker(create_mock_ticker("BTC-PERP", 30000, 30001, 30000.5, ts))
    mock_hl_api.set_mock_ticker(create_mock_ticker("BTC-PERP", 30010, 30011, 30010.5, ts))

    # 2. Trigger Global Circuit Breaker Directly
    global_breaker_name = "global_api_error"  # Name used in reset
    trip_reason = "Test global trip"
    global_breaker = circuit_breaker_system.get_breaker(global_breaker_name)
    assert global_breaker is not None, f"Global breaker '{global_breaker_name}' not found."
    logger.info(f"Tripping global breaker: {global_breaker_name}")
    global_breaker.trip(trip_reason)

    # Verify the global state is OPEN via can_execute
    can_exec, reason = circuit_breaker_system.can_execute(exchange="global")
    assert can_exec is False, f"Global breaker should be open, reason: {reason}"
    assert trip_reason in reason

    # 3. Attempt Execution via execute_opportunity (Corrected method name)
    logger.info("Attempting execution with globally tripped breaker...")
    # Wrap basic_opportunity in a SizedOpportunity
    sized_opportunity_for_test = SizedOpportunity(
        opportunity=basic_opportunity,
        long_size=Decimal("100"),  # Placeholder size
        short_size=Decimal("100"),  # Placeholder size
        allocation_percentage=0.1,  # Placeholder float (10%)
        expected_profit=Decimal("1"),  # Placeholder profit
        expected_return=0.01,  # Placeholder float (1%)
        risk_adjusted_return=0.01,  # Placeholder float
    )
    execution_result = await execution_handler.execute_opportunity(
        sized_opportunity_for_test
    )  # Pass SizedOpportunity

    # 4. Verify Rejection (Restored Assertions)
    assert isinstance(execution_result, TradeExecution), "Expected a TradeExecution result object"
    logger.info(
        f"Received execution result: Status={execution_result.status}, Error='{execution_result.error_message}'"
    )

    assert execution_result.status == ExecutionStatus.REJECTED, (
        f"Expected REJECTED status, got {execution_result.status}"
    )
    assert execution_result.error_message is not None
    assert trip_reason in execution_result.error_message, (
        f"Expected trip reason '{trip_reason}' in message '{execution_result.error_message}'"
    )

    # Verify no orders were placed (mocks shouldn't have been called)
    assert not await mock_bp_api.get_open_orders()
    assert not await mock_hl_api.get_open_orders()
    logger.info("Global circuit breaker test passed.")


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
    basic_opportunity: ArbitrageOpportunity,
):
    """Tests that an open exchange circuit breaker prevents executions involving that exchange."""
    # 1. Setup - Basic state, no initial errors
    mock_bp_api.reset()
    mock_hl_api.reset()
    real_portfolio_tracker.reset()
    # Explicitly reset breakers associated with the system
    circuit_breaker_system.reset_breaker("global_api_error")  # Reset global
    circuit_breaker_system.reset_exchange_breakers("mock_bp")  # Reset exchange specific
    circuit_breaker_system.reset_exchange_breakers("mock_hl")

    mock_bp_api.set_mock_balance(
        Balance(asset="USDC", total=Decimal("10000"), available=Decimal("10000"))
    )
    mock_hl_api.set_mock_balance(
        Balance(asset="USD", total=Decimal("10000"), available=Decimal("10000"))
    )
    await real_portfolio_tracker.initialize()
    ts = datetime.now(UTC)
    mock_bp_api.set_mock_ticker(create_mock_ticker("BTC-PERP", 30000, 30001, 30000.5, ts))
    mock_hl_api.set_mock_ticker(create_mock_ticker("BTC-PERP", 30010, 30011, 30010.5, ts))

    # 2. Trigger Exchange Circuit Breaker Directly (for long exchange)
    target_exchange = basic_opportunity.long_exchange  # e.g., "mock_bp"
    breaker_type = "api_errors"  # Type implied by config/reset/record calls
    trip_reason = f"Test exchange trip for {target_exchange}"

    exchange_breaker = circuit_breaker_system.get_exchange_breaker(target_exchange, breaker_type)
    assert exchange_breaker is not None, (
        f"Exchange breaker '{target_exchange}/{breaker_type}' not found."
    )
    logger.info(f"Tripping exchange breaker: {target_exchange}/{breaker_type}")
    exchange_breaker.trip(trip_reason)

    # Verify the specific exchange breaker is open using can_execute
    can_exec_target, reason_target = circuit_breaker_system.can_execute(exchange=target_exchange)
    assert can_exec_target is False, (
        f"{target_exchange} breaker should be open, reason: {reason_target}"
    )
    assert trip_reason in reason_target

    # Verify global is still closed
    can_exec_global, reason_global = circuit_breaker_system.can_execute(exchange="global")
    assert can_exec_global is True, f"Global breaker should remain closed, reason: {reason_global}"

    # 3. Attempt Execution involving the tripped exchange via execute_opportunity (Corrected method name)
    logger.info(f"Attempting execution with {target_exchange} breaker tripped...")
    # Wrap basic_opportunity in a SizedOpportunity
    sized_opportunity_for_test = SizedOpportunity(
        opportunity=basic_opportunity,
        long_size=Decimal("100"),  # Placeholder size
        short_size=Decimal("100"),  # Placeholder size
        allocation_percentage=0.1,  # Placeholder float (10%)
        expected_profit=Decimal("1"),  # Placeholder profit
        expected_return=0.01,  # Placeholder float (1%)
        risk_adjusted_return=0.01,  # Placeholder float
    )
    execution_result = await execution_handler.execute_opportunity(
        sized_opportunity_for_test
    )  # Pass SizedOpportunity

    # 4. Verify Rejection (Restored Assertions)
    assert isinstance(execution_result, TradeExecution), "Expected a TradeExecution result object"
    logger.info(
        f"Received execution result: Status={execution_result.status}, Error='{execution_result.error_message}'"
    )

    assert execution_result.status == ExecutionStatus.REJECTED, (
        f"Expected REJECTED status, got {execution_result.status}"
    )
    assert execution_result.error_message is not None
    assert trip_reason in execution_result.error_message, (
        f"Expected trip reason '{trip_reason}' in message '{execution_result.error_message}'"
    )

    # Verify no orders were placed
    assert not await mock_bp_api.get_open_orders()
    assert not await mock_hl_api.get_open_orders()

    # 5. Attempt Execution NOT involving the tripped exchange (if possible) - Optional extension
    # logger.info("Exchange circuit breaker test passed.") # Moved to end if step 5 is omitted
    logger.info("Exchange circuit breaker test passed.")


@pytest.mark.asyncio
async def test_funding_rate_validator_reduces_size(
    mock_config,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    real_portfolio_tracker: PortfolioTracker,
    data_handler,
    risk_manager: RiskManager,
    funding_rate_validator: MagicMock,  # Inject the mock validator fixture
    basic_opportunity: ArbitrageOpportunity,  # Use the basic opportunity fixture
):
    """Tests that poor validation metrics reduce the calculated position size."""
    # === ADDED Setup ===
    # Ensure APIs are registered on the tracker
    if "mock_hl" not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client("mock_hl", mock_hl_api)
    if "mock_bp" not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client("mock_bp", mock_bp_api)

    # Set balances on mock APIs
    mock_bp_api.set_mock_balance(
        Balance(asset="USDC", total=Decimal("10000"), available=Decimal("10000"))
    )
    mock_hl_api.set_mock_balance(
        Balance(asset="USD", total=Decimal("10000"), available=Decimal("10000"))
    )

    # Initialize the tracker to fetch balances
    await real_portfolio_tracker.initialize()

    # *** Force re-initialization to ensure config is current ***
    # This might pick up the updated mock_config collateral_asset
    logger.info("Forcing re-initialization of portfolio tracker before baseline check...")
    await real_portfolio_tracker.initialize()
    logger.info(f"Tracker config after re-init: {real_portfolio_tracker.config.config_data}")
    # === END Added Setup ===

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
    baseline_sized_opportunities = risk_manager.validate_opportunities([basic_opportunity])
    risk_manager.funding_rate_validator = funding_rate_validator  # Restore validator

    assert len(baseline_sized_opportunities) == 1, "Baseline sizing failed"
    baseline_size = baseline_sized_opportunities[0].long_size  # Use long_size as representative
    assert baseline_size > 0

    # 3. Calculate size with validator active (expecting reduction)
    validated_sized_opportunities = risk_manager.validate_opportunities([basic_opportunity])

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
    assert validated_size < baseline_size, "Validated size should be smaller than baseline"

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
    # === ADDED: Modify config for this test ===
    # Ensure reconciler only checks the mock exchanges used in this test
    mock_config.config_data["exchanges"] = {
        "mock_hl": {
            "enabled": True,
            "symbols": {"BTC-PERP": "BTC-PERP"},  # Use internal symbol mapping\
            "websocket": mock_config.config_data.get("exchanges", {})
            .get("hyperliquid", {})
            .get("websocket"),  # Reuse existing websocket config if possible\
        },
        "mock_bp": {
            "enabled": True,
            "symbols": {"BTC-PERP": "BTC-PERP"},
            "websocket": mock_config.config_data.get("exchanges", {})
            .get("backpack", {})
            .get("websocket"),
        },
    }
    # Re-initialize the reconciler with the modified config?
    # No, the fixture uses the original mock_config. We need to adjust the test
    # OR create a specific reconciler fixture. Let's adjust the test logic.
    # The reconciler fixture passes the *original* mock_config. \
    # We need the reconciler to get the *correct* list of exchanges.\
    # Let's adjust the *mock_config fixture itself* before the reconciler uses it.\
    # NO - fixtures are evaluated before the test. Modifying in the test is too late.\
    # Alternative: Pass the correct exchanges list *directly* to check_positions if possible.\
    # Looking at PositionReconciliationSystem.check_positions - it iterates based on config.\
    # Simplest Fix: Ensure the reconciler fixture itself uses a config appropriate for integration tests.\
    # Let's modify the fixture in tests/integration/conftest.py instead.\
    # REVERTING THIS EDIT - will apply to tests/integration/conftest.py\

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
    # Register *both* mock APIs to ensure reconciler checks them
    if "mock_bp" not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client("mock_bp", mock_bp_api)
    if "mock_hl" not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client("mock_hl", mock_hl_api)

    # Correct method name: check_positions()
    # Force the check to bypass interval caching
    discrepancies_result = await position_reconciler.check_positions(force=True)
    discrepancies = discrepancies_result.get(exchange_id, {}).get("discrepancies", [])

    # 3. Verify Discrepancy Detection
    assert len(discrepancies) > 0, "Expected reconciler to find discrepancies"

    found_missing_in_tracker = False
    for disc in discrepancies:
        # Check using the actual keys from the discrepancy dictionary
        if (
            disc.get("symbol") == symbol
            and disc.get("type") == "size"  # Check 'type' key
            # Compare Decimal values correctly
            and Decimal(disc.get("exchange_value", "0")) == mock_position.size
            and Decimal(disc.get("local_value", "-1")) == Decimal("0")  # Check local is 0
        ):  # Fixed closing parenthesis and removed extra checks
            found_missing_in_tracker = True
            break

    assert found_missing_in_tracker, "Did not find the expected 'missing_in_tracker' discrepancy"

    # 4. Setup Reverse Scenario - Position in tracker, not on exchange
    real_portfolio_tracker.reset()
    mock_bp_api.reset()  # Clear position from mock API
    real_portfolio_tracker.update_position(exchange_id, mock_position)  # Add to real tracker

    # Ensure API clients are registered on real tracker for the reverse scenario
    if "mock_bp" not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client("mock_bp", mock_bp_api)
    if "mock_hl" not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client("mock_hl", mock_hl_api)

    # === ADDED State Check Logging ===
    logger.info("--- Reverse Scenario State Check ---")
    bp_positions_after_reset = await mock_bp_api.get_positions()
    logger.info(f"Mock BP positions after reset: {bp_positions_after_reset}")
    local_positions_after_update = real_portfolio_tracker.get_positions_by_exchange(exchange_id)
    logger.info(f"Local positions after update: {local_positions_after_update}")
    logger.info("--- End Reverse Scenario State Check ---")
    # === END State Check Logging ===

    # Correct method name: check_positions()
    # Force the check to bypass interval caching
    discrepancies_reverse_result = await position_reconciler.check_positions(force=True)
    discrepancies_reverse = discrepancies_reverse_result.get(exchange_id, {}).get(
        "discrepancies", []
    )

    # 5. Verify Reverse Discrepancy Detection
    assert len(discrepancies_reverse) > 0, "Expected reconciler to find discrepancies (reverse)"
    found_missing_on_exchange = False
    for disc in discrepancies_reverse:
        # Check for size discrepancy where exchange is 0 and local matches mock
        if (
            disc.get("symbol") == symbol
            and disc.get("type") == "size"  # Check 'type' key
            and Decimal(disc.get("exchange_value", "-1")) == Decimal("0")  # Check exchange is 0
            and Decimal(disc.get("local_value", "0")) == mock_position.size  # Check local matches
        ):
            found_missing_on_exchange = True
            break

    assert found_missing_on_exchange, "Did not find the expected 'missing_on_exchange' discrepancy"
