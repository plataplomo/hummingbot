"""
V2 Funding Arbitrage Strategy with Controller
Uses FundingArbitrageController for portfolio-level management of multi-token funding arbitrage
"""
from pathlib import Path

from hummingbot.core.data_type.common import MarketDict
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase


class FundingArbitrageWithControllerConfig(StrategyV2ConfigBase):
    """Configuration for funding arbitrage strategy using controllers"""
    script_file_name: str = Path(__file__).name

    # Controller configuration files
    controllers_config: list[str] = ["funding_arbitrage_controller_config.yml"]

    # Markets will be populated by controller
    markets: MarketDict = {}
    candles_config: list[CandlesConfig] = []


class FundingArbitrageWithController(StrategyV2Base):
    """
    Thin orchestration layer for funding arbitrage using controller.
    All decision logic is delegated to the FundingArbitrageController.
    """

    def __init__(self, config: FundingArbitrageWithControllerConfig):
        super().__init__(config)
        self.config = config

    def on_tick(self):
        """
        Main strategy tick - delegates all work to controllers.
        The controller will:
        1. Scan for funding opportunities across all configured tokens/exchanges
        2. Rank opportunities by profitability
        3. Allocate capital intelligently
        4. Create/stop executors as needed
        5. Manage global risk limits
        """
        # The parent class handles controller orchestration automatically
        super().on_tick()

    def format_status(self) -> str:
        """
        Format strategy status.
        Most information comes from the controller's status.
        """
        lines = ["=== Funding Arbitrage Strategy (Controller-Based) ===\n"]

        # Add controller status
        if self.controllers:
            for controller in self.controllers:
                controller_status = controller.to_format_status()
                lines.extend(controller_status)

        return "\n".join(lines)
