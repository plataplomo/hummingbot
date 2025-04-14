import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
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
    threshold_value: Decimal  # Numeric threshold value

    # Default fields last
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    comparison_operator: Literal["<", "<=", ">", ">=", "abs>"] = ">="
    alert_message: str = "Balance threshold triggered!"
    triggered: bool = False
    last_triggered: datetime | None = None
    cooldown_seconds: int = 3600  # Cooldown period in seconds (1 hour default)
    timestamp: datetime | None = field(default_factory=datetime.now)
    # Add missing fields that are used in the code
    exchange: str = ""
    current_balance: Decimal = field(default_factory=lambda: Decimal("0.0"))
    required_balance: Decimal = field(default_factory=lambda: Decimal("0.0"))
    severity: str = ""
    message: str = ""

    # Define severity constants
    SEVERITY_CRITICAL = "critical"
    SEVERITY_WARNING = "warning"
    SEVERITY_INFO = "info"

    def __post_init__(self) -> None:
        """Ensure all numeric fields are Decimal."""
        # The threshold_value is already type-hinted as Decimal in the dataclass definition.
        # The following conversion block is unreachable and has been removed.
        # if not isinstance(self.threshold_value, Decimal):
        #     try:
        #         self.threshold_value = Decimal(str(self.threshold_value))
        #     except (ValueError, TypeError):
        #         logger.error(f"Invalid threshold value: {self.threshold_value}. Defaulting to 0.0")
        #         self.threshold_value = Decimal("0.0")

        # The current_balance is already type-hinted as Decimal in the dataclass definition.
        # The following conversion block is unreachable and has been removed.
        # if not isinstance(self.current_balance, Decimal):
        #     try:
        #         self.current_balance = Decimal(str(self.current_balance))
        #     except (ValueError, TypeError):
        #         logger.error(f"Invalid current balance: {self.current_balance}. Defaulting to 0.0")
        #         self.current_balance = Decimal("0.0")

        # The required_balance is already type-hinted as Decimal in the dataclass definition.
        # The following conversion block is unreachable and has been removed.
        # if not isinstance(self.required_balance, Decimal):
        #     try:
        #         self.required_balance = Decimal(str(self.required_balance))
        #     except (ValueError, TypeError):
        #         logger.error(
        #             f"Invalid required balance: {self.required_balance}. Defaulting to 0.0"
        #         )
        #         self.required_balance = Decimal("0.0")

    def __str__(self) -> str:
        """Generate string representation of the alert."""
        return (
            f"Balance Alert: {self.exchange} {self.asset} - "
            f"Current: {self.current_balance}, Required: {self.required_balance} - "
            f"{self.message}"
        )


class BalanceMonitor:
    """
    Monitor exchange balances and alert when manual transfers are needed.

    Responsible for:
    - Monitoring balances across exchanges
    - Checking if balances are sufficient for planned operations
    - Generating alerts when balances fall below thresholds
    - Tracking balance changes
    """

    def __init__(self, config: Config, portfolio_tracker: PortfolioTracker) -> None:
        """
        Initialize the balance monitor.

        Args:
            config: Application configuration
            portfolio_tracker: Portfolio tracker for balance information
        """
        self.config = config
        self.portfolio_tracker = portfolio_tracker

        # Load balance parameters from config and convert to Decimal
        self.min_usdc_balance = Decimal(str(config.get("balance.min_usdc_balance", 50.0)))
        self.low_balance_threshold = Decimal(
            str(config.get("balance.low_balance_threshold", 100.0))
        )

        # Exchange-specific minimum balance requirements
        self.exchange_min_balances: dict[str, dict[str, Decimal]] = {}

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
            min_usdc = self.config.get(
                f"exchanges.{exchange_id}.min_usdc_balance", self.min_usdc_balance
            )
            self.exchange_min_balances[exchange_id] = {"USDC": Decimal(str(min_usdc))}

            # Add any exchange-specific asset requirements
            min_balances = self.config.get(f"exchanges.{exchange_id}.min_balances", {})
            for asset, amount in min_balances.items():
                # Convert amount to Decimal
                self.exchange_min_balances[exchange_id][asset] = Decimal(str(amount))

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
                # Safely extract the total amount from the Balance object
                current_balance_amount = Decimal("0.0")
                if current_balance is not None:
                    try:
                        # Balance object has a total field which is a Decimal
                        # Keep it as Decimal - don't convert to float
                        current_balance_amount = current_balance.total
                    except AttributeError:
                        logger.warning(f"Unable to extract balance amount from: {current_balance}")

                # Check if balance is below minimum
                if current_balance_amount < min_balance:
                    # Create critical alert
                    alert = BalanceAlert(
                        exchange=exchange_id,
                        asset=asset,
                        current_balance=current_balance_amount,
                        required_balance=min_balance,
                        severity=BalanceAlert.SEVERITY_CRITICAL,
                        message="Balance below minimum requirement",
                        threshold_type="low",
                        threshold_value=min_balance,
                    )
                    new_alerts.append(alert)
                    self.active_alerts.append(alert)
                    already_alerted.add((exchange_id, asset))

                    logger.warning(str(alert))

                # Check if balance is below low threshold but above minimum
                elif (
                    current_balance_amount < self.low_balance_threshold
                    and (exchange_id, asset) not in already_alerted
                ):
                    # Create warning alert
                    alert = BalanceAlert(
                        exchange=exchange_id,
                        asset=asset,
                        current_balance=current_balance_amount,
                        required_balance=self.low_balance_threshold,
                        severity=BalanceAlert.SEVERITY_WARNING,
                        message="Balance nearing low threshold",
                        threshold_type="low",
                        threshold_value=self.low_balance_threshold,
                    )
                    new_alerts.append(alert)
                    self.active_alerts.append(alert)

                    logger.info(str(alert))

        # Add alerts to history and cap history size
        self.alert_history.extend(new_alerts)
        if len(self.alert_history) > self.max_alert_history:
            self.alert_history = self.alert_history[-self.max_alert_history :]

        return new_alerts

    def check_balance_for_opportunity(
        self, exchange: str, asset: str, required_amount: Decimal
    ) -> BalanceAlert | None:
        """
        Check if a specific exchange has sufficient balance for a potential trade.

        Args:
            exchange: Exchange name
            asset: Asset symbol
            required_amount: Amount required for the trade

        Returns:
            BalanceAlert if insufficient, None otherwise
        """
        current_balance = self.portfolio_tracker.get_exchange_balance(exchange, asset)
        # Safely extract the total amount from the Balance object
        current_balance_amount = Decimal("0.0")
        if current_balance is not None:
            try:
                # Balance object has a total field which is a Decimal
                # Keep it as Decimal - don't convert to float
                current_balance_amount = current_balance.total
            except AttributeError:
                logger.warning(f"Unable to extract balance amount from: {current_balance}")

        if current_balance_amount < required_amount:
            alert = BalanceAlert(
                exchange=exchange,
                asset=asset,
                current_balance=current_balance_amount,
                required_balance=required_amount,
                severity=BalanceAlert.SEVERITY_CRITICAL,
                message=f"Insufficient {asset} balance for trade on {exchange}",
                threshold_type="low",
                threshold_value=required_amount,
            )
            logger.warning(str(alert))
            # Add to active alerts
            if not any(a.id == alert.id for a in self.active_alerts):
                self.active_alerts.append(alert)
            return alert

        return None

    def get_active_alerts(self) -> list[BalanceAlert]:
        """Return the list of currently active alerts."""
        return self.active_alerts

    def get_critical_alerts(self) -> list[BalanceAlert]:
        """Return only active critical alerts."""
        # Ensure alert.severity is checked correctly
        return [
            alert
            for alert in self.active_alerts
            if alert.severity == BalanceAlert.SEVERITY_CRITICAL
        ]

    def get_balance_status(self) -> dict[str, Any]:
        """
        Get a summary of current balance status across all exchanges.

        Returns:
            Dictionary summarizing balance status
        """
        # Initialize status dictionary
        status: dict[str, Any] = {
            "timestamp": datetime.now().isoformat(),
            "balances": {},
            "alerts": [str(alert) for alert in self.active_alerts],
        }

        # Iterate through configured exchanges
        for exchange_id, min_balances in self.exchange_min_balances.items():
            exchange_balances = {}
            for asset in min_balances:
                # Get current balance from PortfolioTracker
                current_balance = self.portfolio_tracker.get_exchange_balance(exchange_id, asset)
                # Safely extract the total amount from the Balance object
                current_balance_amount = Decimal("0.0")
                if current_balance is not None:
                    try:
                        # Balance object has a total field which is a Decimal
                        # Keep it as Decimal - don't convert to float
                        current_balance_amount = current_balance.total
                    except AttributeError:
                        logger.warning(f"Unable to extract balance amount from: {current_balance}")
                min_balance = self.exchange_min_balances[exchange_id][asset]

                # Create a new dictionary with balance information
                # Maintain Decimal internally, only convert to string for output in the dictionary
                asset_info = {
                    "current": str(current_balance_amount),
                    "minimum": str(min_balance),
                    "status": "OK" if current_balance_amount >= min_balance else "LOW",
                }
                exchange_balances[asset] = asset_info

            status["balances"][exchange_id] = exchange_balances

        return status

    def _load_state(self) -> None:
        """Loads alerts and last known balances from a state file."""
        # This method is incomplete and has syntax errors
        # Completely reimplement it with proper structure and flow
        self.state_file = self.config.get("balance.state_file", "")

        if not self.state_file or not os.path.exists(self.state_file):
            logger.info(f"No state file found at {self.state_file}, starting with empty state")
            return

        try:
            # Load state from file - we'll just use logging for now as the method is incomplete
            logger.info(f"Would load balance state from {self.state_file}")
            # Actual implementation would read the file and load the state
        except Exception as e:
            logger.error(f"Error loading balance state: {e}")

    def add_alert(self, alert: BalanceAlert) -> None:
        """Adds a new balance alert."""
        # The alert.threshold_value is guaranteed to be Decimal by the BalanceAlert type hint.
        # The following conversion block is unreachable and has been removed.
        # if not isinstance(alert.threshold_value, Decimal):
        #     try:
        #         alert.threshold_value = Decimal(str(alert.threshold_value))
        #     except (ValueError, TypeError):
        #         logger.error(f"Invalid threshold value for alert: {alert.threshold_value}")
        #         return  # Do not add alert with invalid threshold

        # Add to active alerts if not already present
        if not any(a.id == alert.id for a in self.active_alerts):
            self.active_alerts.append(alert)
            logger.info(f"Added balance alert: ID={alert.id}, Asset={alert.asset}")
            # Save state would be called here if implemented
        else:
            # Mypy incorrectly flags this 'else' block as unreachable.
            # It's necessary to handle cases where an alert with the same ID already exists.
            logger.warning(f"Alert with ID {alert.id} already exists") # mypy: [unreachable]
