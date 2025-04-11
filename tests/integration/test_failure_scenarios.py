"""
Integration tests focusing on failure scenarios and safety system triggers.
"""

import logging
import pytest
from decimal import Decimal
from datetime import datetime, timezone
import asyncio

# Configure logging for tests
logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Import core components and models
from cyberdelta.core.execution_handler import ExecutionHandler, ExecutionStatus
from cyberdelta.core.models import ArbitrageOpportunity, Balance
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.validation.circuit_breaker import (
    CircuitBreakerSystem,
    BreakerState,
)  # Import BreakerState
from cyberdelta.apis.base import APIError  # Import APIError for simulation

# Import mocks and test utilities
from tests.integration.mocks.mock_exchange import MockExchangeAPI
from tests.integration.conftest import create_mock_ticker  # Assuming this helper exists

# Fixtures will be reused from tests/integration/conftest.py


@pytest.mark.usefixtures(
    "mock_config",
    "mock_hl_api",
    "mock_bp_api",
    "real_portfolio_tracker",
    "circuit_breaker_system",
    "execution_handler",
    "basic_opportunity",
)
class TestFailureScenarios:
    """Groups failure scenario integration tests."""

    @pytest.mark.asyncio
    async def test_cb_trips_on_repeated_api_errors(
        self,
        mock_config,  # Required for setup
        mock_hl_api: MockExchangeAPI,
        mock_bp_api: MockExchangeAPI,
        real_portfolio_tracker: PortfolioTracker,
        circuit_breaker_system: CircuitBreakerSystem,
        execution_handler: ExecutionHandler,
        basic_opportunity: ArbitrageOpportunity,
    ):
        """Tests that repeated API errors trigger the exchange circuit breaker."""
        # 1. Setup
        target_exchange = "mock_bp"  # Exchange we will cause to fail
        other_exchange = "mock_hl"
        target_breaker_type = "api_errors"

        mock_bp_api.reset()
        mock_hl_api.reset()
        real_portfolio_tracker.reset()
        circuit_breaker_system.reset_breaker("global_api_error")
        circuit_breaker_system.reset_exchange_breakers(target_exchange)
        circuit_breaker_system.reset_exchange_breakers(other_exchange)

        # Set balances and initialize tracker
        mock_bp_api.set_mock_balance(
            Balance(asset="USDC", total=Decimal("10000"), free=Decimal("10000"))
        )
        mock_hl_api.set_mock_balance(
            Balance(asset="USD", total=Decimal("10000"), free=Decimal("10000"))
        )
        await real_portfolio_tracker.initialize()
        ts = datetime.now(timezone.utc)
        mock_bp_api.set_mock_ticker(
            create_mock_ticker("BTC-PERP", 30000, 30001, 30000.5, ts)
        )
        mock_hl_api.set_mock_ticker(
            create_mock_ticker("BTC-PERP", 30010, 30011, 30010.5, ts)
        )

        # Configure mock_bp to consistently fail order placement
        error_message = "Simulated API error during order placement"
        mock_bp_api.set_error_simulation(
            APIError(error_message, exchange_code=target_exchange),
            method_name="place_order",
        )
        logger.info(
            f"Configured {target_exchange} mock to fail place_order with: {error_message}"
        )

        # Create a sized opportunity (details don't matter much as it should fail)
        sized_opportunity = SizedOpportunity(
            opportunity=basic_opportunity,  # basic_opportunity uses mock_bp as long
            long_size=Decimal("1000"),
            short_size=Decimal("1000"),
            allocation_percentage=0.1,
            expected_profit=Decimal("10"),
            expected_return=0.01,
            risk_adjusted_return=0.01,
        )

        # 2. Trigger Failures until Breaker Trips
        breaker = circuit_breaker_system.get_exchange_breaker(
            target_exchange, target_breaker_type
        )
        assert breaker is not None, "Target exchange breaker not found"
        max_failures_to_trip = (
            breaker.error_threshold
        )  # Get threshold from the breaker instance

        logger.info(
            f"Breaker '{breaker.name}' threshold: {max_failures_to_trip}. Current state: {breaker.state.name}"
        )
        execution_results = []
        for i in range(max_failures_to_trip + 1):
            logger.info(f"Execution attempt {i + 1}/{max_failures_to_trip + 1}")
            result = await execution_handler.execute_opportunity(sized_opportunity)
            execution_results.append(result)
            # Small delay to allow CB state updates if needed (though sync should be fast here)
            await asyncio.sleep(0.01)
            if breaker.state == BreakerState.OPEN:
                logger.info(f"Breaker tripped after {i + 1} attempts.")
                break
        else:  # This else belongs to the for loop
            pytest.fail(
                f"Circuit breaker did not trip after {max_failures_to_trip + 1} attempts."
            )

        # 3. Verify Breaker State and Rejection
        assert breaker.state == BreakerState.OPEN, "Breaker should be OPEN"
        # Check the last execution result specifically (the one that should have been rejected)
        last_result = execution_results[-1]
        assert last_result.status == ExecutionStatus.REJECTED, (
            f"Expected REJECTED status after trip, got {last_result.status}"
        )
        assert breaker.name in last_result.error_message, (
            "Error message should mention the tripped breaker"
        )
        logger.info(
            f"Verified breaker is OPEN and last execution was REJECTED. Error: {last_result.error_message}"
        )

        # 4. Attempt another execution - Should still be REJECTED
        logger.info("Attempting one more execution while breaker is OPEN...")
        rejected_result = await execution_handler.execute_opportunity(sized_opportunity)
        assert rejected_result.status == ExecutionStatus.REJECTED, (
            "Execution should still be REJECTED"
        )
        assert breaker.name in rejected_result.error_message, (
            "Error message should still mention the tripped breaker"
        )
        logger.info("Verified subsequent execution is also REJECTED.")

        # 5. Clean up mock error simulation
        mock_bp_api.clear_error_simulation()
        logger.info(f"Cleared error simulation for {target_exchange}.")

    # More failure test cases can be added below

    # Test cases will be added here
    pass
