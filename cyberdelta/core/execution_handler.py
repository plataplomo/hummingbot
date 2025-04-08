import asyncio
import logging
from typing import Dict, List, Optional

# Assuming structure: strategy_math/coding_strategy/src/
from ..config.settings import settings
from ..core.models import ArbitrageOpportunity, Order, OrderSide, OrderType
from ..core.portfolio_tracker import PortfolioTracker # To update order status
from ..apis.base import ExchangeAPI, APIError

logger = logging.getLogger(__name__)

class ExecutionHandler:
    """Handles the execution of trading opportunities."""

    def __init__(self, api_clients: Dict[str, ExchangeAPI], portfolio_tracker: PortfolioTracker):
        self.api_clients = api_clients
        self.portfolio_tracker = portfolio_tracker
        self._active_executions: Dict[str, asyncio.Task] = {} # Track ongoing executions

    async def execute_opportunity(self, opportunity: ArbitrageOpportunity):
        """Attempts to execute a sized arbitrage opportunity."""
        if not opportunity or opportunity.recommended_size <= 0:
            logger.warning("Attempted to execute invalid or zero-size opportunity.")
            return

        # Avoid executing the same opportunity multiple times concurrently
        if opportunity.opportunity_id in self._active_executions:
            logger.debug(f"Execution for opportunity {opportunity.opportunity_id} already in progress.")
            return

        logger.info(f"Executing opportunity: {opportunity.opportunity_id} | Size: {opportunity.recommended_size:.6f}")
        exec_task = asyncio.create_task(self._execute_legs(opportunity))
        self._active_executions[opportunity.opportunity_id] = exec_task

        # Clean up task entry when done
        def task_done_callback(task):
            try:
                # Check for exceptions
                _ = task.result()
            except asyncio.CancelledError:
                 logger.info(f"Execution task for {opportunity.opportunity_id} cancelled.")
            except Exception as e:
                logger.error(f"Execution task for {opportunity.opportunity_id} failed: {e}", exc_info=True)
            finally:
                if opportunity.opportunity_id in self._active_executions:
                    del self._active_executions[opportunity.opportunity_id]
        exec_task.add_done_callback(task_done_callback)


    async def _execute_legs(self, opportunity: ArbitrageOpportunity):
        """Internal method to execute the individual legs of an opportunity."""
        # TODO: Implement sophisticated leg execution (parallel, sequential with checks, error handling, partial fills)
        # This is a simplified placeholder assuming two legs for cross-exchange

        if opportunity.opportunity_type != 'cross' or len(opportunity.legs) != 2:
            logger.error(f"Execution handler currently only supports 2-leg cross opportunities. Opp: {opportunity.opportunity_id}")
            return

        leg1_str = opportunity.legs[0]
        leg2_str = opportunity.legs[1]
        symbol1, ex1 = leg1_str.split('@')
        symbol2, ex2 = leg2_str.split('@')

        client1 = self.api_clients.get(ex1)
        client2 = self.api_clients.get(ex2)

        if not client1 or not client2:
            logger.error(f"Missing API client for opportunity {opportunity.opportunity_id}. Ex1: {ex1}, Ex2: {ex2}")
            return

        side1 = opportunity.execution_details.get('side1')
        side2 = opportunity.execution_details.get('side2')
        quantity = opportunity.recommended_size # Assume size is consistent for now

        if not side1 or not side2:
             logger.error(f"Missing execution details (sides) for opportunity {opportunity.opportunity_id}")
             return

        # --- Simple Sequential Execution (Placeholder - Prone to Legging Risk!) --- #
        order1: Optional[Order] = None
        order2: Optional[Order] = None
        try:
            logger.info(f"Placing Leg 1 ({side1}) on {ex1} for {symbol1} Qty: {quantity}")
            order1 = await client1.place_order(
                symbol=symbol1,
                side=side1,
                order_type=OrderType.MARKET, # Use MARKET for simplicity, consider LIMIT
                quantity=quantity
            )
            # Immediately update portfolio tracker (optimistic update)
            await self.portfolio_tracker.update_order(ex1, order1)
            logger.info(f"Leg 1 placed on {ex1}. Order ID: {order1.order_id}")

            # TODO: Wait for confirmation/partial fill of leg 1 before placing leg 2?
            # Add delay modeling?
            await asyncio.sleep(0.5) # Small artificial delay

            logger.info(f"Placing Leg 2 ({side2}) on {ex2} for {symbol2} Qty: {quantity}")
            order2 = await client2.place_order(
                symbol=symbol2,
                side=side2,
                order_type=OrderType.MARKET,
                quantity=quantity
            )
            await self.portfolio_tracker.update_order(ex2, order2)
            logger.info(f"Leg 2 placed on {ex2}. Order ID: {order2.order_id}")

            # TODO: Monitor order statuses until filled or failed
            # Need mechanism to handle partial fills (e.g., reduce other leg or cancel)
            logger.info(f"Execution attempt for {opportunity.opportunity_id} completed (order placement only).")

        except APIError as e:
            logger.error(f"API Error during execution of {opportunity.opportunity_id}: {e}")
            # --- Compensation Logic (Placeholder) --- #
            # If leg 1 placed but leg 2 failed, try to cancel leg 1 or market close it
            if order1 and order1.status != OrderStatus.FILLED and order1.status != OrderStatus.CANCELED:
                logger.warning(f"Attempting to cancel Leg 1 ({ex1}, {order1.order_id}) due to Leg 2 failure.")
                try:
                    await client1.cancel_order(order1.order_id, symbol1)
                except Exception as cancel_e:
                    logger.error(f"Failed to cancel Leg 1 ({ex1}, {order1.order_id}): {cancel_e}")
                    # Consider market closing the position as a fallback
        except NotImplementedError as e:
             logger.error(f"Execution failed for {opportunity.opportunity_id}: API method not implemented. {e}")
        except Exception as e:
            logger.error(f"Unexpected error during execution of {opportunity.opportunity_id}: {e}", exc_info=True)
            # Add compensation logic if needed

    # --- Run loop (optional - could process execution queue) ---
    # async def run(self):
    #     logger.info("Execution Handler starting.")
    #     while True:
    #         # Check queue for opportunities to execute?
    #         await asyncio.sleep(1)

    # No stop event needed if execution is triggered externally

    # No stop event needed if execution is triggered externally 