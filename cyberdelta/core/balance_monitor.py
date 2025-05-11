import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, cast

from cyberdelta.core.models import SpotBalance  # Changed import
from cyberdelta.core.portfolio_tracker import (
    PortfolioTracker,  # Updated from Balance
)
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
        # Validations removed as fields are type-hinted correctly with defaults

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
        self.state_file: str = ""  # Initialize state_file attribute

        # Load balance parameters from config and convert to Decimal
        # DEFENSIVE CHECK: Convert config values safely
        try:
            self.min_usdc_balance = Decimal(str(config.get("balance.min_usdc_balance", "50.0")))
            self.low_balance_threshold = Decimal(
                str(config.get("balance.low_balance_threshold", "100.0"))
            )
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid balance config value: {e}. Using defaults.")
            self.min_usdc_balance = Decimal("50.0")
            self.low_balance_threshold = Decimal("100.0")

        # Exchange-specific minimum balance requirements
        self.exchange_min_balances: dict[str, dict[str, Decimal]] = {}

        # Active alerts
        self.active_alerts: list[BalanceAlert] = []

        # Historical alerts (keep last N)
        self.alert_history: list[BalanceAlert] = []
        # Use config for max history size
        raw_max_history = config.get("balance.max_alert_history", 100)
        try:
            self.max_alert_history = int(str(raw_max_history))
        except (ValueError, TypeError):
            logger.warning(f"Invalid max_alert_history '{raw_max_history}'. Using default 100.")
            self.max_alert_history = 100

        # Load exchange-specific requirements
        self._load_exchange_requirements()
        # Load state if configured
        self._load_state()

    def _load_exchange_requirements(self) -> None:
        """Load exchange-specific balance requirements from config."""
        # Cast config result to expected dict type
        exchanges_conf = self.config.get("exchanges", {})
        exchanges_dict = cast(
            dict[str, Any], exchanges_conf if isinstance(exchanges_conf, dict) else {}
        )

        for exchange_id in exchanges_dict.keys():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue

            # Default minimum USDC balance for each exchange
            min_usdc_raw = self.config.get(
                f"exchanges.{exchange_id}.min_usdc_balance", self.min_usdc_balance
            )
            try:
                min_usdc = Decimal(str(min_usdc_raw))
            except (ValueError, TypeError):
                logger.warning(
                    f"Invalid min_usdc_balance '{min_usdc_raw}' for {exchange_id}. Using global default."
                )
                min_usdc = self.min_usdc_balance

            self.exchange_min_balances[exchange_id] = {"USDC": min_usdc}

            # Add any exchange-specific asset requirements
            min_balances_conf = self.config.get(f"exchanges.{exchange_id}.min_balances", {})
            min_balances_dict = cast(
                dict[str, Any], min_balances_conf if isinstance(min_balances_conf, dict) else {}
            )

            for asset, amount_raw in min_balances_dict.items():
                try:
                    # Convert amount to Decimal
                    amount_decimal = Decimal(str(amount_raw))
                    self.exchange_min_balances[exchange_id][asset] = amount_decimal
                except (ValueError, TypeError):
                    logger.error(
                        f"Invalid min_balance amount '{amount_raw}' for {asset} on {exchange_id}. Skipping."
                    )

    def check_balances(self) -> list[BalanceAlert]:
        """
        Check all exchange balances against requirements.

        Returns:
            List of balance alerts
        """
        new_alerts: list[BalanceAlert] = []  # Explicit type annotation
        already_alerted: set[tuple[str, str]] = set()  # Track exchanges/assets already alerted

        self.active_alerts = []  # Clear active alerts before checking

        for exchange_id, min_balances in self.exchange_min_balances.items():
            for asset, min_balance in min_balances.items():
                # Use the correct type hint: SpotBalance
                current_balance: SpotBalance | None = self.portfolio_tracker.get_exchange_balance(
                    exchange_id, asset
                )
                available_balance = (
                    current_balance.available_quantity  # Direct attribute access
                    if current_balance is not None  # Check if balance exists
                    else Decimal("0.0")  # Default if balance is None
                )

                if available_balance < min_balance:
                    alert = BalanceAlert(
                        exchange=exchange_id,
                        asset=asset,
                        current_balance=available_balance,  # Known Decimal
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
                    available_balance < self.low_balance_threshold
                    and (exchange_id, asset) not in already_alerted
                ):
                    alert = BalanceAlert(
                        exchange=exchange_id,
                        asset=asset,
                        current_balance=available_balance,  # Known Decimal
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
        # Use the correct type hint: SpotBalance
        current_balance: SpotBalance | None = self.portfolio_tracker.get_exchange_balance(
            exchange, asset
        )
        available_balance = (
            current_balance.available_quantity  # Direct attribute access
            if current_balance is not None  # Check if balance exists
            else Decimal("0.0")  # Default if balance is None
        )

        min_balance_req = self.exchange_min_balances.get(exchange, {}).get(asset, Decimal("0.0"))

        if available_balance < required_amount:
            alert = BalanceAlert(
                exchange=exchange,
                asset=asset,
                current_balance=available_balance,  # Known Decimal
                required_balance=required_amount,
                severity=BalanceAlert.SEVERITY_CRITICAL,
                message=f"Insufficient {asset} balance for trade on {exchange}",
                threshold_type="low",
                threshold_value=required_amount,
            )
            logger.warning(str(alert))
            # Add to active alerts only if not already present (check ID)
            if not any(a.id == alert.id for a in self.active_alerts):
                self.active_alerts.append(alert)
            return alert

        return None

    def get_active_alerts(self) -> list[BalanceAlert]:
        """Return the list of currently active alerts."""
        return self.active_alerts

    def get_critical_alerts(self) -> list[BalanceAlert]:
        """Return only active critical alerts."""
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
        status: dict[str, Any] = {
            "timestamp": datetime.now().isoformat(),
            "balances": {},
            "alerts": [str(alert) for alert in self.active_alerts],
        }

        for exchange_id, min_balances in self.exchange_min_balances.items():
            exchange_balances: dict[str, dict[str, str]] = {}  # Type hint for inner dict
            for asset in min_balances:
                # Use the correct type hint: SpotBalance
                current_balance: SpotBalance | None = self.portfolio_tracker.get_exchange_balance(
                    exchange_id, asset
                )
                available_balance = (
                    current_balance.available_quantity  # Direct attribute access
                    if current_balance is not None  # Check if balance exists
                    else Decimal("0.0")  # Default if balance is None
                )

                # Create a new dictionary with balance information (strings for JSON compatibility)
                asset_info: dict[str, str] = {
                    "current": str(available_balance),
                    "minimum": str(min_balance_req_loop),  # Use correct min balance
                    "status": "OK"
                    if available_balance >= min_balance_req_loop
                    else "LOW",  # Use correct min balance
                }
                exchange_balances[asset] = asset_info

            status["balances"][exchange_id] = exchange_balances

        return status

    def _load_state(self) -> None:
        """Loads alerts and last known balances from a state file."""
        # Fetch config value safely
        state_file_conf = self.config.get("balance.state_file", "")
        self.state_file = str(state_file_conf) if isinstance(state_file_conf, str) else ""

        if not self.state_file or not os.path.exists(self.state_file):
            logger.info(f"No state file found at {self.state_file}, starting with empty state")
            return

        try:
            # Placeholder for loading state
            logger.info(f"Attempting to load balance state from {self.state_file}")
            # Example: Read and parse JSON, update self.active_alerts, self.alert_history
        except Exception as e:
            logger.error(f"Error loading balance state from {self.state_file}: {e}")

    def add_alert(self, alert: BalanceAlert) -> None:
        """Adds a new balance alert, preventing duplicates."""
        # Alert properties are validated by BalanceAlert dataclass
        if not any(a.id == alert.id for a in self.active_alerts):
            self.active_alerts.append(alert)
            logger.info(f"Added balance alert: ID={alert.id}, Asset={alert.asset}")
            # self._save_state() # TODO: Implement state saving
        else:
            logger.warning(f"Alert with ID {alert.id} already exists, not adding again.")

    # TODO: Implement state saving if needed
