"""
Data types for the Funding Arbitrage Executor
"""
from decimal import Decimal
from typing import Literal

from pydantic import Field
from pydantic.dataclasses import dataclass

from hummingbot.core.data_type.common import OrderType
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import TripleBarrierConfig

# Type aliases for reconciliation state
ReconciliationStage = Literal["none", "limit", "market", "emergency"]
MissingLeg = Literal["long", "short"]
PositionSide = Literal["long", "short"]


@dataclass
class ReconciliationState:
    """State tracking for position reconciliation with type-safe literals"""
    stage: ReconciliationStage = Field(default="none", description="Reconciliation stage")
    attempts: int = Field(default=0, ge=0, description="Number of reconciliation attempts")
    last_attempt_time: float | None = Field(default=None, description="Timestamp of last attempt")
    missing_leg: MissingLeg | None = Field(default=None, description="Which leg needs reconciliation")


class FundingArbitrageExecutorConfig(ExecutorConfigBase):
    """Configuration for the Funding Arbitrage Executor"""
    type: Literal["funding_arbitrage_executor"] = "funding_arbitrage_executor"

    # Token being arbitraged
    token: str

    # Long position configuration
    long_connector_name: str
    long_trading_pair: str

    # Short position configuration
    short_connector_name: str
    short_trading_pair: str

    # Position sizing
    position_size_quote: Decimal
    leverage: int = 20

    # Entry configuration
    open_order_type: OrderType = OrderType.LIMIT
    entry_timeout: int = 30  # Max seconds to wait for both positions to fill

    # Exit configuration
    triple_barrier_config: TripleBarrierConfig

    # Safety parameters
    max_unhedged_exposure_time: int = 30  # Max seconds of unhedged exposure
    warning_exposure_time: int = 10  # Warn after this many seconds
    emergency_use_market_orders: bool = True  # Use market orders for emergency hedge

    # Funding rate thresholds
    min_funding_rate_diff: Decimal = Decimal("0.001")
    funding_rate_diff_stop_loss: Decimal = Decimal("-0.001")
