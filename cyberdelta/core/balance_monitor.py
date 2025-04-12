import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.utils.config import Config

logger = logging.getLogger(__name__)


@dataclass
class BalanceAlert:
    """Represents a configured balance alert threshold."""

    # Non-default fields first
    asset: str
    threshold_type: Literal["low", "high", "change"]  # Type of threshold
    threshold_value: float  # Numeric threshold value

    # Default fields last
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    comparison_operator: Literal["<", "<=", ">", ">=", "abs>"] = ">="
    alert_message: str = "Balance threshold triggered!"
    triggered: bool = False
    last_triggered: datetime | None = None
    cooldown_seconds: int = 3600  # Cooldown period in seconds (1 hour default)
    # Changed: Make timestamp optional
    # timestamp: datetime = field(default_factory=datetime.now) # Timestamp of creation/last update
    timestamp: datetime | None = field(default_factory=datetime.now)


class BalanceMonitor:
    """
    Monitor exchange balances and alert when manual transfers are needed.

    Responsible for:
    - Monitoring balances across exchanges
    - Checking if balances are sufficient for planned operations
    - Generating alerts when balances fall below thresholds
    - Tracking balance changes
    """

    def __init__(self, config: Config, portfolio_tracker: PortfolioTracker):
        """
        Initialize the balance monitor.

        Args:
            config: Application configuration
            portfolio_tracker: Portfolio tracker for balance information
        """
        self.config = config
        self.portfolio_tracker = portfolio_tracker

        # Load balance parameters from config
        self.min_usdc_balance = config.get("balance.min_usdc_balance", 50.0)
        self.low_balance_threshold = config.get("balance.low_balance_threshold", 100.0)

        # Exchange-specific minimum balance requirements
        self.exchange_min_balances: dict[str, dict[str, float]] = {}

        # Active alerts
        self.active_alerts: list[BalanceAlert] = []

        # Historical alerts (keep last 100)
        self.alert_history: list[BalanceAlert] = []
        self.max_alert_history = 100

        # Load exchange-specific requirements
        self._load_exchange_requirements()

    def _load_exchange_requirements(self) -> None:
        """Load exchange-specific balance requirements from config."""
        for exchange_id in self.config.get("exchanges", {}).keys():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue

            # Default minimum USDC balance for each exchange
            self.exchange_min_balances[exchange_id] = {
                "USDC": self.config.get(
                    f"exchanges.{exchange_id}.min_usdc_balance", self.min_usdc_balance
                )
            }

            # Add any exchange-specific asset requirements
            min_balances = self.config.get(f"exchanges.{exchange_id}.min_balances", {})
            for asset, amount in min_balances.items():
                self.exchange_min_balances[exchange_id][asset] = amount

    def check_balances(self) -> list[BalanceAlert]:
        """
        Check all exchange balances against requirements.

        Returns:
            List of balance alerts
        """
        new_alerts = []
        already_alerted = set()  # Track exchanges/assets already alerted

        # Clear active alerts
        self.active_alerts = []

        # Check each exchange's balances
        for exchange_id, min_balances in self.exchange_min_balances.items():
            for asset, min_balance in min_balances.items():
                current_balance = self.portfolio_tracker.get_exchange_balance(exchange_id, asset)

                # Check if balance is below minimum
                if current_balance < min_balance:
                    # Create critical alert
                    alert = BalanceAlert(
                        exchange=exchange_id,
                        asset=asset,
                        current_balance=current_balance,
                        required_balance=min_balance,
                        severity=BalanceAlert.SEVERITY_CRITICAL,
                        message="Balance below minimum requirement",
                    )
                    new_alerts.append(alert)
                    self.active_alerts.append(alert)
                    already_alerted.add((exchange_id, asset))

                    logger.warning(str(alert))

                # Check if balance is below low threshold but above minimum
                elif (
                    current_balance < self.low_balance_threshold
                    and (exchange_id, asset) not in already_alerted
                ):
                    # Create warning alert
                    alert = BalanceAlert(
                        exchange=exchange_id,
                        asset=asset,
                        current_balance=current_balance,
                        required_balance=self.low_balance_threshold,
                        severity=BalanceAlert.SEVERITY_WARNING,
                        message="Balance below warning threshold",
                    )
                    new_alerts.append(alert)
                    self.active_alerts.append(alert)

                    logger.info(str(alert))

        # Add new alerts to history
        self.alert_history.extend(new_alerts)

        # Trim history if needed
        if len(self.alert_history) > self.max_alert_history:
            self.alert_history = self.alert_history[-self.max_alert_history :]

        return new_alerts

    def check_balance_for_opportunity(
        self, exchange_id: str, asset: str, required_amount: float
    ) -> BalanceAlert | None:
        """
        Check if a specific exchange has sufficient balance for an opportunity.

        Args:
            exchange_id: Exchange identifier
            asset: Asset name
            required_amount: Required balance amount

        Returns:
            BalanceAlert if balance is insufficient, None otherwise
        """
        current_balance = self.portfolio_tracker.get_exchange_balance(exchange_id, asset)

        if current_balance < required_amount:
            # Create alert
            alert = BalanceAlert(
                exchange=exchange_id,
                asset=asset,
                current_balance=current_balance,
                required_balance=required_amount,
                severity=BalanceAlert.SEVERITY_WARNING,
                message="Insufficient balance for planned opportunity",
            )

            # Don't add to active alerts since this is an opportunity-specific check
            # But do log it
            logger.warning(str(alert))

            return alert

        return None

    def get_active_alerts(self) -> list[BalanceAlert]:
        """
        Get all active balance alerts.

        Returns:
            List of active alerts
        """
        return self.active_alerts.copy()

    def get_critical_alerts(self) -> list[BalanceAlert]:
        """
        Get critical balance alerts.

        Returns:
            List of critical alerts
        """
        return [
            alert
            for alert in self.active_alerts
            if alert.severity == BalanceAlert.SEVERITY_CRITICAL
        ]

    def get_balance_status(self) -> dict[str, Any]:
        """
        Get a summary of current balance status.

        Returns:
            Dictionary with balance status information
        """
        status = {
            "balances": {},
            "alerts": len(self.active_alerts),
            "critical_alerts": len(self.get_critical_alerts()),
            "all_balances_ok": len(self.active_alerts) == 0,
        }

        # Add balance information for each exchange
        for exchange_id in self.exchange_min_balances.keys():
            exchange_balances = {}

            for asset in self.exchange_min_balances[exchange_id].keys():
                current_balance = self.portfolio_tracker.get_exchange_balance(exchange_id, asset)
                min_balance = self.exchange_min_balances[exchange_id][asset]

                exchange_balances[asset] = {
                    "current": current_balance,
                    "minimum": min_balance,
                    "status": "OK" if current_balance >= min_balance else "LOW",
                }

            status["balances"][exchange_id] = exchange_balances

        return status

    def _load_state(self) -> None:
        """Loads alerts and last known balances from a state file."""
        if not self.state_file or not os.path.exists(self.state_file):
            # Convert Decimal balance to float for comparison
            balance_float = float(balance.available)

            if alert.threshold_type == "low" and eval(
                f"balance_float {alert.comparison_operator} alert.threshold_value"
            ):
                # Pass float balance to trigger
                self._trigger_alert(alert, balance_float)
            elif alert.threshold_type == "high" and eval(
                f"balance_float {alert.comparison_operator} alert.threshold_value"
            ):
                # Pass float balance to trigger
                self._trigger_alert(alert, balance_float)
            elif alert.threshold_type == "change":
                last_known = self.last_known_balances.get(alert.asset)
                if last_known is not None:
                    # Ensure last_known is also float for comparison
                    last_known_float = float(last_known.available)
                    change = abs(balance_float - last_known_float)
                    if eval(f"change {alert.comparison_operator} alert.threshold_value"):
                        # Pass float balance to trigger
                        self._trigger_alert(alert, balance_float)

            # Update last known balance (store Decimal)
            # ... existing code ...

    def add_alert(self, alert: BalanceAlert) -> None:
        """Adds a new balance alert."""
        # Ensure threshold value is float
        if not isinstance(alert.threshold_value, float):
            try:
                alert.threshold_value = float(alert.threshold_value)
            except ValueError:
                self.logger.error("Invalid threshold value for alert", alert=alert)
                return

        # Corrected: Use append for list
        # Original: self.alerts[alert.id] = alert
        # Assuming self.alerts is a list, check for duplicates first
        if not any(a.id == alert.id for a in self.alerts):
            self.alerts.append(alert)
            self.logger.info("Added balance alert", alert_id=alert.id, asset=alert.asset)
            self._save_state()
        else:
            self.logger.warning("Alert with this ID already exists", alert_id=alert.id)
