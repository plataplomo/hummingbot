"""
Integration tests focusing on failure scenarios and safety system triggers.
"""

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import pytest

# Import core components and models
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.execution_handler import ExecutionHandler, ExecutionStatus, TradeExecution
from cyberdelta.core.models import (
    SpotBalance,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.utils.config import Config  # Added import
from cyberdelta.validation import ArbitrageOpportunity
from cyberdelta.validation.circuit_breaker import (
    APIErrorBreaker,  # Import specific breaker type
    BreakerState,
    CircuitBreakerSystem,
)  # Import BreakerState

# Import mocks and test utilities
from tests.integration.conftest import create_mock_ticker  # Assuming this helper exists
from tests.integration.mocks.mock_exchange import MockExchangeAPI

# Configure logging for tests
logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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
        mock_config: Config,  # Added Config type hint
        mock_hl_api: MockExchangeAPI,
        mock_bp_api: MockExchangeAPI,
        real_portfolio_tracker: PortfolioTracker,
        circuit_breaker_system: CircuitBreakerSystem,
        execution_handler: ExecutionHandler,
        basic_opportunity: ArbitrageOpportunity,
    ) -> None:  # Added return type hint
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
        now = datetime.now(UTC)  # Need timestamp
        mock_bp_api.set_mock_balance(
            SpotBalance(
                exchange="mock_bp",
                asset="USDT",
                # total=Decimal("10000"), # Use correct fields
                # available=Decimal("10000"),
                total_quantity=Decimal("10000"),  # Add missing
                available_quantity=Decimal("10000"),  # Add missing
                timestamp=now,  # Add missing
            )
        )
        mock_hl_api.set_mock_balance(
            SpotBalance(
                exchange="mock_hl",
                asset="USDT",
                # total=Decimal("10000"), # Use correct fields
                # available=Decimal("10000"),
                total_quantity=Decimal("10000"),  # Add missing
                available_quantity=Decimal("10000"),  # Add missing
                timestamp=now,  # Add missing
            )
        )
        await real_portfolio_tracker.initialize()
        # ts = datetime.now(UTC) # Moved 'now' up
        # Ensure BOTH exchanges have valid tickers configured *before* error simulation
        bp_symbol = str(mock_config.get(f"exchanges.{target_exchange}.symbols.BTC"))  # Cast to str
        hl_symbol = str(mock_config.get(f"exchanges.{other_exchange}.symbols.BTC"))  # Cast to str

        mock_bp_api.set_mock_ticker(create_mock_ticker(bp_symbol, 30000, 30001, 30000.5, now))
        mock_hl_api.set_mock_ticker(create_mock_ticker(hl_symbol, 30010, 30011, 30010.5, now))

        # Configure mock_bp to consistently fail order placement
        error_message = "Simulated API error during order placement"
        mock_bp_api.set_error_simulation(
            APIError(
                message=error_message,
                code=APIErrorCode.EXCHANGE_SPECIFIC.value,  # Add missing code
                exchange_code=target_exchange,
            ),
            method_name="place_order",
        )
        logger.info(f"Configured {target_exchange} mock to fail place_order with: {error_message}")

        # Create a sized opportunity (details don't matter much as it should fail)
        sized_opportunity = SizedOpportunity(
            opportunity=basic_opportunity,  # basic_opportunity uses mock_bp as long
            long_size=Decimal("1000"),
            short_size=Decimal("1000"),
            allocation_percentage=Decimal("0.1"),  # Use Decimal
            expected_profit=Decimal("10"),
            expected_return=Decimal("0.01"),  # Use Decimal
            risk_adjusted_return=Decimal("0.01"),  # Use Decimal
        )

        # 2. Trigger Failures until Breaker Trips
        breaker = circuit_breaker_system.get_exchange_breaker(target_exchange, target_breaker_type)
        assert breaker is not None, "Target exchange breaker not found"
        # Cast to specific type to access error_threshold
        api_breaker = cast(APIErrorBreaker, breaker)
        max_failures_to_trip: int = (
            api_breaker.error_threshold
        )  # Get threshold from the breaker instance

        logger.info(
            f"Breaker '{api_breaker.name}' threshold: {max_failures_to_trip}. Current state: {api_breaker.state.name}"
        )
        execution_results: list[TradeExecution] = []  # Add type hint
        for i in range(max_failures_to_trip + 1):  # Now max_failures_to_trip is int
            logger.info(f"Execution attempt {i + 1}/{max_failures_to_trip + 1}")
            result = await execution_handler.execute_opportunity(sized_opportunity)
            execution_results.append(result)
            # Small delay to allow CB state updates if needed (though sync should be fast here)
            await asyncio.sleep(0.1)
            if breaker.state == BreakerState.OPEN:
                logger.info(f"Breaker tripped after {i + 1} attempts.")
                break
        else:  # This else belongs to the for loop
            pytest.fail(f"Circuit breaker did not trip after {max_failures_to_trip + 1} attempts.")

        # 3. Verify Breaker State
        assert breaker.state == BreakerState.OPEN, "Breaker should be OPEN"

        # --- MODIFIED: Attempt execution *after* breaker is confirmed OPEN ---
        logger.info(
            f"Breaker {api_breaker.name} is confirmed OPEN. Attempting one more execution..."
        )
        rejected_result = await execution_handler.execute_opportunity(sized_opportunity)
        logger.info(f"Result of execution attempt while OPEN: {rejected_result.status.name}")

        # 4. Verify Rejection
        assert rejected_result.status == ExecutionStatus.REJECTED, (
            f"Expected REJECTED status after trip, got {rejected_result.status.name}"
        )
        # Construct the fully qualified name expected in the error message
        expected_breaker_name_in_message = f"exchange:{target_exchange}:{target_breaker_type}"
        # Add None check before 'in'
        assert (
            rejected_result.error_message is not None
            and expected_breaker_name_in_message in rejected_result.error_message
        ), (
            f"Error message '{rejected_result.error_message}' should mention the tripped breaker '{expected_breaker_name_in_message}'"
        )

        # 5. Verify Other Exchange Unaffected
        # Try executing on the other exchange (should succeed if its breaker didn't trip)
        # Temporarily disable the error on the failing exchange to test the other one
        mock_bp_api.clear_error()
        circuit_breaker_system.reset_breaker(
            f"exchange:{target_exchange}:{target_breaker_type}"
        )  # Reset the tripped breaker for this check

        # Create an opportunity targeting the *other* exchange
        _other_long_symbol = mock_config.get(f"exchanges.{other_exchange}.symbols.BTC")
        _other_short_symbol = mock_config.get(
            f"exchanges.{target_exchange}.symbols.BTC"
        )  # Needs a symbol, even if CB might block it

        # Correctly instantiate ArbitrageOpportunity with required args
        other_opportunity = ArbitrageOpportunity(
            symbol="BTC",  # Corrected argument name
            long_exchange=other_exchange,
            short_exchange=target_exchange,
            long_price=Decimal("30010"),
            short_price=Decimal("30000"),  # Prices reversed
            long_funding_rate=Decimal("0.0001"),  # Dummy value
            short_funding_rate=Decimal("-0.00005"),  # Dummy value
            net_funding_differential=Decimal("-0.0001"),  # Dummy value
            timestamp=datetime.now(UTC),
            basis_volatility=float(Decimal("0.01")),  # Cast to float
            expected_profit=Decimal("10.0"),  # Use Decimal
            # Removed incorrect/unexpected arguments:
            # internal_symbol, long_symbol, short_symbol, book_imbalance
        )

        # Create a sized opportunity for the other exchange
        other_sized_opportunity = SizedOpportunity(
            opportunity=other_opportunity,
            long_size=Decimal("1000"),
            short_size=Decimal("1000"),
            allocation_percentage=Decimal("0.1"),  # Use Decimal
            expected_profit=Decimal("10"),
            expected_return=Decimal("0.01"),  # Use Decimal
            risk_adjusted_return=Decimal("0.01"),  # Use Decimal
        )

        # Re-enable the breaker for the target exchange before the next attempt
        # circuit_breaker_system.force_trip(f"exchange:{target_exchange}:{target_breaker_type}", "Re-tripped for test")
        # No, we keep it reset to test if the *other* exchange works

        logger.info(
            f"Attempting execution on the other exchange ({other_exchange}) to ensure it's unaffected..."
        )
        # Ensure the other exchange's breaker is CLOSED before attempting
        other_breaker = circuit_breaker_system.get_exchange_breaker(
            other_exchange, target_breaker_type
        )
        assert other_breaker is not None
        assert other_breaker.state == BreakerState.CLOSED, (
            f"Other exchange breaker {other_breaker.name} should be CLOSED"
        )

        other_result = await execution_handler.execute_opportunity(other_sized_opportunity)
        logger.info(f"Result of execution attempt on other exchange: {other_result.status.name}")

        # We expect the other exchange's leg to succeed, but the overall might still fail
        # if the target_exchange's breaker blocks *its* leg during execution.
        # Let's check the status isn't REJECTED due to the *other* exchange's breaker.
        assert other_result.status != ExecutionStatus.REJECTED or (
            other_result.status == ExecutionStatus.REJECTED
            # Add None check before 'not in'
            and other_result.error_message is not None
            and f"exchange:{other_exchange}:" not in other_result.error_message
        ), (
            f"Execution on unaffected exchange {other_exchange} was unexpectedly REJECTED by its own breaker: {other_result.error_message}"
        )

        # More precise check: if it failed, it should be because the *original* breaker (mock_bp)
        # might still be tripped or the mock_bp API fails again.
        # If the status is FAILED, check the error message relates to mock_bp
        if other_result.status == ExecutionStatus.FAILED:
            # Add None check before 'in'
            assert (
                other_result.error_message is not None
                and target_exchange in other_result.error_message
            ), (
                f"Execution failed, but error message '{other_result.error_message}' doesn't mention the originally failing exchange '{target_exchange}'"
            )
        # If the status is REJECTED, check the error message relates to mock_bp's breaker
        elif other_result.status == ExecutionStatus.REJECTED:
            # Add None check before 'in'
            assert (
                other_result.error_message is not None
                and f"exchange:{target_exchange}:" in other_result.error_message
            ), (
                f"Execution rejected, but error message '{other_result.error_message}' doesn't mention the originally failing exchange breaker '{target_exchange}'"
            )
        # Otherwise (COMPLETED), it means mock_bp API didn't fail this time *and* its breaker was reset/didn't re-trip instantly.
        else:
            assert other_result.status == ExecutionStatus.COMPLETED, (  # Use COMPLETED
                f"Expected {other_exchange} execution to succeed after {target_exchange} failure and reset, got {other_result.status.name}"
            )
            # This assertion might be too strict if success has no error message
            # assert (
            #     other_result.error_message is not None
            #     and target_exchange in other_result.error_message
            # ), (
            #     f"Error message '{other_result.error_message}' doesn\'t mention the originally failing exchange breaker '{target_exchange}'"
            # )

        # 6. Reset Breaker Manually (Optional Check)
        # circuit_breaker_system.reset_breaker(f"exchange:{target_exchange}:{target_breaker_type}")
        # assert breaker.state == BreakerState.CLOSED, "Breaker should be CLOSED after manual reset"
        # logger.info(f"Breaker {breaker.name} manually reset.")

        # Optional: Attempt execution again after reset, should pass if API error is removed
        # mock_bp_api.clear_error_simulation("place_order")
        # reset_success_result = await execution_handler.execute_opportunity(sized_opportunity)
        # assert reset_success_result.status == ExecutionStatus.SUCCESS, \
        #     f"Execution failed after reset and error removal: {reset_success_result.error_message}"
        # logger.info("Execution successful after manual reset and error removal.")

    # More failure test cases can be added below

    # Test cases will be added here
    pass
