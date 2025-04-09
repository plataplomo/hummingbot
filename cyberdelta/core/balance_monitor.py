import logging
from typing import Dict, List, Optional, Any, Set
from datetime import datetime

from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.utils.config import Config

logger = logging.getLogger(__name__)

class BalanceAlert:
    """
    Alert for balance conditions that require attention.
    """
    
    SEVERITY_INFO = "INFO"
    SEVERITY_WARNING = "WARNING"
    SEVERITY_CRITICAL = "CRITICAL"
    
    def __init__(self, 
                 exchange: str, 
                 asset: str, 
                 current_balance: float,
                 required_balance: float,
                 severity: str,
                 message: str,
                 timestamp: datetime = None):
        """
        Initialize a balance alert.
        
        Args:
            exchange: Exchange identifier
            asset: Asset name
            current_balance: Current balance
            required_balance: Required balance
            severity: Alert severity level
            message: Alert message
            timestamp: Alert timestamp
        """
        self.exchange = exchange
        self.asset = asset
        self.current_balance = current_balance
        self.required_balance = required_balance
        self.severity = severity
        self.message = message
        self.timestamp = timestamp or datetime.now()
    
    def __str__(self) -> str:
        """String representation of the alert."""
        return f"[{self.severity}] {self.exchange}/{self.asset}: {self.message} (Current: {self.current_balance:.2f}, Required: {self.required_balance:.2f})"


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
        self.min_usdc_balance = config.get('balance.min_usdc_balance', 50.0)
        self.low_balance_threshold = config.get('balance.low_balance_threshold', 100.0)
        
        # Exchange-specific minimum balance requirements
        self.exchange_min_balances: Dict[str, Dict[str, float]] = {}
        
        # Active alerts
        self.active_alerts: List[BalanceAlert] = []
        
        # Historical alerts (keep last 100)
        self.alert_history: List[BalanceAlert] = []
        self.max_alert_history = 100
        
        # Load exchange-specific requirements
        self._load_exchange_requirements()
    
    def _load_exchange_requirements(self):
        """Load exchange-specific balance requirements from config."""
        for exchange_id in self.config.get('exchanges', {}).keys():
            if not self.config.get(f'exchanges.{exchange_id}.enabled', False):
                continue
                
            # Default minimum USDC balance for each exchange
            self.exchange_min_balances[exchange_id] = {
                'USDC': self.config.get(f'exchanges.{exchange_id}.min_usdc_balance', 
                                      self.min_usdc_balance)
            }
            
            # Add any exchange-specific asset requirements
            min_balances = self.config.get(f'exchanges.{exchange_id}.min_balances', {})
            for asset, amount in min_balances.items():
                self.exchange_min_balances[exchange_id][asset] = amount
    
    def check_balances(self) -> List[BalanceAlert]:
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
                        message=f"Balance below minimum requirement"
                    )
                    new_alerts.append(alert)
                    self.active_alerts.append(alert)
                    already_alerted.add((exchange_id, asset))
                    
                    logger.warning(str(alert))
                
                # Check if balance is below low threshold but above minimum
                elif current_balance < self.low_balance_threshold and (exchange_id, asset) not in already_alerted:
                    # Create warning alert
                    alert = BalanceAlert(
                        exchange=exchange_id,
                        asset=asset,
                        current_balance=current_balance,
                        required_balance=self.low_balance_threshold,
                        severity=BalanceAlert.SEVERITY_WARNING,
                        message=f"Balance below warning threshold"
                    )
                    new_alerts.append(alert)
                    self.active_alerts.append(alert)
                    
                    logger.info(str(alert))
        
        # Add new alerts to history
        self.alert_history.extend(new_alerts)
        
        # Trim history if needed
        if len(self.alert_history) > self.max_alert_history:
            self.alert_history = self.alert_history[-self.max_alert_history:]
        
        return new_alerts
    
    def check_balance_for_opportunity(self, 
                                     exchange_id: str, 
                                     asset: str, 
                                     required_amount: float) -> Optional[BalanceAlert]:
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
                message=f"Insufficient balance for planned opportunity"
            )
            
            # Don't add to active alerts since this is an opportunity-specific check
            # But do log it
            logger.warning(str(alert))
            
            return alert
        
        return None
    
    def get_active_alerts(self) -> List[BalanceAlert]:
        """
        Get all active balance alerts.
        
        Returns:
            List of active alerts
        """
        return self.active_alerts.copy()
    
    def get_critical_alerts(self) -> List[BalanceAlert]:
        """
        Get critical balance alerts.
        
        Returns:
            List of critical alerts
        """
        return [alert for alert in self.active_alerts if alert.severity == BalanceAlert.SEVERITY_CRITICAL]
    
    def get_balance_status(self) -> Dict[str, Any]:
        """
        Get a summary of current balance status.
        
        Returns:
            Dictionary with balance status information
        """
        status = {
            'balances': {},
            'alerts': len(self.active_alerts),
            'critical_alerts': len(self.get_critical_alerts()),
            'all_balances_ok': len(self.active_alerts) == 0
        }
        
        # Add balance information for each exchange
        for exchange_id in self.exchange_min_balances.keys():
            exchange_balances = {}
            
            for asset in self.exchange_min_balances[exchange_id].keys():
                current_balance = self.portfolio_tracker.get_exchange_balance(exchange_id, asset)
                min_balance = self.exchange_min_balances[exchange_id][asset]
                
                exchange_balances[asset] = {
                    'current': current_balance,
                    'minimum': min_balance,
                    'status': 'OK' if current_balance >= min_balance else 'LOW'
                }
            
            status['balances'][exchange_id] = exchange_balances
        
        return status
