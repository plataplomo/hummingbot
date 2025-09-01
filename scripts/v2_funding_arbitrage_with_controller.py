"""
V2 Funding Arbitrage Strategy with Controller
Uses FundingArbitrageController for portfolio-level management of multi-token funding arbitrage

Features:
- Portfolio-wide risk management across multiple tokens
- Dynamic capital allocation to best opportunities
- Automatic position balancing and rebalancing
- Global drawdown protection
- Performance monitoring and reporting
- Unhedged exposure monitoring and emergency controls
"""
from decimal import Decimal
from pathlib import Path

from pydantic import Field

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import MarketDict, PositionMode
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class FundingArbitrageWithControllerConfig(StrategyV2ConfigBase):
    """Configuration for funding arbitrage strategy using controllers"""
    script_file_name: str = Path(__file__).name

    # Controller configuration files
    controllers_config: list[str] = Field(default_factory=lambda: ["funding_arbitrage_controller_config.yml"])

    # Markets will be populated by controller
    markets: MarketDict = Field(default_factory=MarketDict)
    candles_config: list[CandlesConfig] = Field(default_factory=list)

    # Global risk management
    max_global_drawdown_quote: float | None = 100.0  # Maximum portfolio drawdown
    max_controller_drawdown_quote: float | None = 50.0  # Maximum per-controller drawdown

    # Performance reporting
    performance_report_interval: int = 60  # Report performance every 60 seconds

    # Emergency controls
    emergency_stop_on_unhedged: bool = True  # Stop if positions become unhedged
    max_unhedged_exposure_time: int = 30  # Maximum seconds to allow unhedged exposure


class FundingArbitrageWithController(StrategyV2Base):
    """
    Advanced orchestration layer for funding arbitrage using controller.
    Provides portfolio-level risk management, performance tracking, and emergency controls.
    """

    def __init__(self, connectors: dict[str, ConnectorBase], config: FundingArbitrageWithControllerConfig) -> None:
        self.logger().info("🚀 Initializing FundingArbitrageWithController")
        self.logger().info(f"📊 Available connectors: {list(connectors.keys()) if connectors else 'None'}")
        self.logger().info(f"📁 Controller configs to load: {config.controllers_config}")

        # Debug logging for config loading
        try:
            self.logger().info("⚙️ Loading controller configs...")
            loaded_configs = config.load_controller_configs()
            self.logger().info(f"✅ Successfully loaded {len(loaded_configs)} controller config(s)")
            for cfg in loaded_configs:
                self.logger().info(f"  - Controller: {cfg.controller_name} (type: {cfg.controller_type})")
                self.logger().info(f"    Module: {cfg.__module__}")
                self.logger().info(f"    Class: {cfg.__class__.__name__}")
                self.logger().info(f"    ID: {cfg.id}")
                # Test if we can get the controller class
                try:
                    ctrl_class = cfg.get_controller_class()
                    self.logger().info(f"    Controller class found: {ctrl_class.__name__}")
                except Exception as e:
                    self.logger().error(f"    ❌ Failed to get controller class: {e}")
        except Exception as e:
            self.logger().error(f"❌ Failed to load controller configs: {e}", exc_info=True)

        # Call parent constructor which should initialize controllers
        super().__init__(connectors, config)
        self.config: FundingArbitrageWithControllerConfig = config

        # Validate we have exactly one funding arbitrage controller
        if len(self.controllers) != 1:
            raise ValueError(f"This script expects exactly 1 funding arbitrage controller, got {len(self.controllers)}")

        # Get the single controller
        self.controller_id = next(iter(self.controllers.keys()))
        self.controller = next(iter(self.controllers.values()))

        # Validate it's a funding arbitrage controller
        if self.controller.config.controller_name != "funding_arbitrage_controller":
            raise ValueError(
                f"This script only works with funding_arbitrage_controller, "
                f"got {self.controller.config.controller_name}",
            )

        # Performance tracking for the single controller
        self.max_controller_pnl = Decimal(0)
        self._last_performance_report_timestamp = 0
        self._is_drawdown_exited = False
        self._last_controller_log_time: float = 0

        # Unhedged exposure tracking
        self._unhedged_exposure_start: dict[str, float] = {}
        self._unhedged_warnings_sent: set[str] = set()

        self.logger().info(f"📈 Funding Arbitrage Controller initialized: {self.controller_id}")
        self.logger().info(f"  - Tokens: {self.controller.config.tokens}")
        self.logger().info(f"  - Exchanges: {self.controller.config.connectors}")

    def on_tick(self) -> None:
        """
        Main strategy tick with enhanced monitoring and risk management.
        """
        # Let parent handle controller orchestration
        super().on_tick()

        # Log controller status periodically (every 30 seconds)
        if self.current_timestamp - self._last_controller_log_time > 30:
            self.logger().info(f"📈 Status: {self.controller.status.name}")

            # Try to start controller if not running
            if self.controller.status != RunnableStatus.RUNNING:
                self.logger().warning("⚠️ Controller not running, attempting to start...")
                try:
                    self.controller.start()
                    self.logger().info("✅ Started controller")
                except Exception as e:
                    self.logger().error(f"❌ Failed to start controller: {e}")

            self._last_controller_log_time = self.current_timestamp

        # Additional safety and monitoring
        if not self._is_stop_triggered and not self._is_drawdown_exited:
            self.check_unhedged_exposure()
            self.check_controller_drawdown()
            self.send_performance_report()
            self.check_manual_kill_switch()

    def check_unhedged_exposure(self) -> None:
        """Monitor and handle unhedged exposure across all executors"""
        if not self.config.emergency_stop_on_unhedged:
            return

        executors = self.get_executors_by_controller(self.controller_id)

        for executor in executors:
            if not hasattr(executor, "_has_unhedged_exposure"):
                continue

            executor_id = executor.id
            is_unhedged = executor._has_unhedged_exposure()

            if is_unhedged:
                # Track exposure start time
                if executor_id not in self._unhedged_exposure_start:
                    self._unhedged_exposure_start[executor_id] = self.current_timestamp
                    self.logger().warning(
                        f"⚠️ Unhedged exposure detected for executor {executor_id} "
                        f"(Token: {executor.config.token})",
                    )

                # Check exposure duration
                exposure_duration = self.current_timestamp - self._unhedged_exposure_start[executor_id]

                if exposure_duration > self.config.max_unhedged_exposure_time:
                    self.logger().error(
                        f"🚨 EMERGENCY: Unhedged exposure exceeded {self.config.max_unhedged_exposure_time}s "
                        f"for executor {executor_id}. Triggering emergency stop!",
                    )
                    self._is_stop_triggered = True
                    HummingbotApplication.main_application().stop()
                    return

            elif executor_id in self._unhedged_exposure_start:
                # Exposure resolved
                duration = self.current_timestamp - self._unhedged_exposure_start[executor_id]
                self.logger().info(
                    f"✅ Unhedged exposure resolved for executor {executor_id} "
                    f"after {duration:.1f} seconds",
                )
                del self._unhedged_exposure_start[executor_id]

    def check_controller_drawdown(self) -> None:
        """Check controller drawdown limit"""
        if not self.config.max_controller_drawdown_quote or self._is_drawdown_exited:
            return

        if self.controller.status != RunnableStatus.RUNNING:
            return

        performance_report = self.get_performance_report(self.controller_id)
        if performance_report is None:
            self.logger().debug("No performance report available yet")
            return

        controller_pnl = performance_report.global_pnl_quote

        # Update max PnL if current is higher
        if controller_pnl > self.max_controller_pnl:
            self.max_controller_pnl = controller_pnl
        else:
            # Check drawdown
            current_drawdown = self.max_controller_pnl - controller_pnl
            if current_drawdown > self.config.max_controller_drawdown_quote:
                self.logger().warning(
                    f"Controller reached max drawdown of ${current_drawdown:.2f}. "
                    f"Stopping controller and closing positions.",
                )
                self.controller.stop()

                # Stop all executors
                executors_to_stop = self.get_executors_by_controller(self.controller_id)
                self.executor_orchestrator.execute_actions([
                    StopExecutorAction(
                        controller_id=self.controller_id,
                        executor_id=executor.id,
                    ) for executor in executors_to_stop
                ])
                self._is_drawdown_exited = True

    def send_performance_report(self) -> None:
        """Send periodic performance reports"""
        if self.current_timestamp - self._last_performance_report_timestamp >= self.config.performance_report_interval:
            self.logger().info("=== FUNDING ARBITRAGE PERFORMANCE REPORT ===")

            report = self.get_performance_report(self.controller_id)
            if report is None:
                self.logger().debug("No report available yet")
                return

            controller_pnl = report.global_pnl_quote

            # Count active executors
            executors = self.get_executors_by_controller(self.controller_id)
            active_positions = sum(1 for e in executors if e.is_trading)

            self.logger().info(
                f"PnL: ${controller_pnl:.2f}, Active Positions: {active_positions}",
            )

            # Log individual executor status
            for executor in executors:
                if executor.is_trading:
                    self.logger().info(f"  - {executor.config.token}: Trading")

            self._last_performance_report_timestamp = self.current_timestamp

    def check_manual_kill_switch(self) -> None:
        """Check for manual kill switch activation"""
        if self.controller.config.manual_kill_switch and self.controller.status == RunnableStatus.RUNNING:
            self.logger().info("Manual kill switch activated")
            self.controller.stop()

            # Stop all executors
            executors_to_stop = self.get_executors_by_controller(self.controller_id)
            self.executor_orchestrator.execute_actions([
                StopExecutorAction(
                    executor_id=executor.id,
                    controller_id=self.controller_id,
                ) for executor in executors_to_stop
            ])

    def format_status(self) -> str:
        """
        Format comprehensive strategy status including risk metrics.
        """
        lines = ["=== Funding Arbitrage Strategy ==="]

        # Get metrics
        report = self.get_performance_report(self.controller_id)
        total_pnl = report.global_pnl_quote if report else Decimal(0)
        executors = self.get_executors_by_controller(self.controller_id)
        total_positions = sum(1 for e in executors if e.is_trading)

        lines.extend([
            "\n📊 METRICS:",
            f"  PnL: ${total_pnl:.2f}",
            f"  Max Drawdown Allowed: ${self.config.max_controller_drawdown_quote:.2f}",
            f"  Active Positions: {total_positions}",
            f"  Unhedged Exposures: {len(self._unhedged_exposure_start)}",
        ])

        # Add warnings if any
        if self._unhedged_exposure_start:
            lines.append("\n⚠️ WARNINGS:")
            for executor_id, start_time in self._unhedged_exposure_start.items():
                duration = self.current_timestamp - start_time
                lines.append(f"  - Unhedged exposure for {executor_id}: {duration:.1f}s")

        # Add controller status
        if not self._is_drawdown_exited:
            lines.append(f"\n📈 Controller: {self.controller_id}")
            controller_status = self.controller.to_format_status()
            lines.extend(["  " + line for line in controller_status])
        else:
            lines.append("\n🛑 Controller stopped due to drawdown")

        return "\n".join(lines)

    def apply_initial_setting(self) -> None:
        """Apply initial settings for funding arbitrage trading"""
        super().apply_initial_setting()

        # Initialize performance tracking
        self.max_controller_pnl = Decimal(0)

        # Configure position modes for funding arbitrage
        # Funding arbitrage needs HEDGE mode to hold simultaneous long/short positions
        self.logger().info("🔧 Configuring exchanges for funding arbitrage")

        config_dict = self.controller.config.model_dump()
        leverage = config_dict.get("leverage", 3)

        # Get connectors from the funding arbitrage config
        if "connectors" in config_dict:
            for connector_name in config_dict["connectors"]:
                if self.is_perpetual(connector_name) and connector_name in self.connectors:
                    connector = self.connectors[connector_name]

                    # Check if this exchange supports HEDGE mode
                    supported_modes = connector.supported_position_modes()

                    if PositionMode.HEDGE in supported_modes:
                        try:
                            self.logger().info(
                                f"Setting {connector_name} to HEDGE mode "
                                f"(supports simultaneous long/short)...",
                            )
                            connector.set_position_mode(PositionMode.HEDGE)
                            self.logger().info(f"✅ {connector_name} set to HEDGE mode")
                        except Exception as e:
                            self.logger().error(f"❌ Failed to set HEDGE mode for {connector_name}: {e}")
                    else:
                        self.logger().info(f"[INFO] {connector_name} doesn't support HEDGE mode (using ONEWAY mode)")

                    # Set leverage for each trading pair on this connector
                    try:
                        # Get all trading pairs for this connector from markets config
                        if connector_name in self.config.markets:
                            for trading_pair in self.config.markets[connector_name]:
                                connector.set_leverage(trading_pair, leverage)
                                self.logger().info(f"✅ Set {connector_name} {trading_pair} leverage to {leverage}x")
                        else:
                            self.logger().warning(f"No trading pairs configured for {connector_name}")
                    except Exception as e:
                        self.logger().error(f"Failed to set leverage for {connector_name}: {e}")

    def create_actions_proposal(self) -> list[CreateExecutorAction]:
        """
        Controller-based strategies handle executor actions internally through controllers.
        Returns empty list as required by base class.
        """
        return []

    def stop_actions_proposal(self) -> list[StopExecutorAction]:
        """
        Controller-based strategies handle stop actions internally through controllers.
        Returns empty list as required by base class.
        """
        return []
