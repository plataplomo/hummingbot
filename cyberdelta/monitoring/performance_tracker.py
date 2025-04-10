"""
Performance Monitoring System for CyberDeltaEngine.

This module provides tools for tracking, analyzing, and storing
performance metrics for trading strategies.
"""

import logging
import asyncio
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
import json
import pandas as pd
import numpy as np
from dataclasses import dataclass, asdict, field

from cyberdelta.core.types import TradeSignal, MarketData, SignalType
from cyberdelta.core.signal_generator import ArbitrageOpportunity
from cyberdelta.core.risk_manager import SizedOpportunity

logger = logging.getLogger(__name__)

@dataclass
class PerformanceMetrics:
    """Storage for performance metrics data."""
    strategy_name: str
    timestamp: datetime
    
    # Signal metrics
    signals_generated: int = 0
    signals_executed: int = 0
    signal_quality_score: float = 0.0
    
    # Trading metrics
    trades_executed: int = 0
    trade_win_rate: float = 0.0
    avg_trade_duration: float = 0.0
    
    # PnL metrics
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_pnl: float = 0.0
    
    # Risk metrics
    current_drawdown: float = 0.0
    max_drawdown: float = 0.0
    volatility: float = 0.0
    
    # Ratio metrics
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0

@dataclass
class TradeMetrics:
    """Metrics for an individual trade."""
    trade_id: str
    strategy_name: str
    symbol: str
    exchange: str
    entry_time: datetime
    exit_time: Optional[datetime] = None
    entry_price: float = 0.0
    exit_price: float = 0.0
    direction: str = ""  # "LONG" or "SHORT"
    size: float = 0.0
    pnl: float = 0.0
    pnl_percentage: float = 0.0
    duration: float = 0.0  # In seconds
    execution_quality: float = 0.0  # 0.0 to 1.0
    slippage: float = 0.0
    fees: float = 0.0
    is_completed: bool = False
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class SignalMetrics:
    """Metrics for a trading signal."""
    signal_id: str
    strategy_name: str
    signal_type: str
    symbol: str
    timestamp: datetime
    generation_time: float = 0.0  # Time taken to generate the signal in ms
    confidence_score: float = 0.0  # 0.0 to 1.0
    utility_score: float = 0.0
    expected_profit: float = 0.0
    sized_position: bool = False
    execution_status: str = "PENDING"  # PENDING, EXECUTED, REJECTED, EXPIRED
    execution_latency: float = 0.0  # Time from generation to execution in ms
    metadata: Dict[str, Any] = field(default_factory=dict)


class PerformanceTracker:
    """
    Track and record performance metrics for a trading strategy.
    
    This class hooks into various points in the strategy execution flow to
    collect metrics on signals, trades, and overall performance.
    """
    
    def __init__(self, strategy_name: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the performance tracker.
        
        Args:
            strategy_name: Name of the strategy to track
            config: Configuration parameters
        """
        self.strategy_name = strategy_name
        self.config = config or {}
        
        # Storage for metrics
        self.metrics_history: List[PerformanceMetrics] = []
        self.trade_history: Dict[str, TradeMetrics] = {}
        self.signal_history: Dict[str, SignalMetrics] = {}
        self.opportunity_history: Dict[str, ArbitrageOpportunity] = {}
        self.sized_opportunity_history: Dict[str, SizedOpportunity] = {}
        
        # Current state
        self.current_trades: Dict[str, TradeMetrics] = {}
        self.pending_signals: Dict[str, SignalMetrics] = {}
        
        # Performance summary statistics
        self.total_pnl = 0.0
        self.total_signals_generated = 0
        self.total_signals_executed = 0
        self.total_trades_executed = 0
        self.winning_trades = 0
        self.losing_trades = 0
        
        # Configuration
        self.metrics_interval = self.config.get('metrics_interval', 60)  # seconds
        self.persistence_enabled = self.config.get('persistence_enabled', True)
        self.persistence_path = self.config.get('persistence_path', f"metrics/{self.strategy_name}")
        self.max_history_length = self.config.get('max_history_length', 10000)
        
        # Start metrics collection task
        if self.config.get('auto_collect', True):
            self._start_metrics_collection()
    
    def _start_metrics_collection(self):
        """Start the background task for periodic metrics collection."""
        asyncio.create_task(self._periodic_metrics_collection())
    
    async def _periodic_metrics_collection(self):
        """Periodically collect and store performance metrics."""
        while True:
            try:
                self.collect_metrics()
                
                # Persist metrics if enabled
                if self.persistence_enabled:
                    self.persist_metrics()
                
                # Prune history if it exceeds the maximum length
                self._prune_history()
                
                # Wait for the next collection interval
                await asyncio.sleep(self.metrics_interval)
            except Exception as e:
                logger.error(f"Error in periodic metrics collection: {e}", exc_info=True)
                await asyncio.sleep(10)  # Wait a bit before retrying
    
    def collect_metrics(self) -> PerformanceMetrics:
        """
        Collect current performance metrics.
        
        Returns:
            PerformanceMetrics object with current metrics
        """
        metrics = PerformanceMetrics(
            strategy_name=self.strategy_name,
            timestamp=datetime.now(),
            signals_generated=self.total_signals_generated,
            signals_executed=self.total_signals_executed,
            trades_executed=self.total_trades_executed,
            realized_pnl=self.total_pnl
        )
        
        # Calculate trade win rate
        total_completed_trades = self.winning_trades + self.losing_trades
        if total_completed_trades > 0:
            metrics.trade_win_rate = (self.winning_trades / total_completed_trades) * 100
        
        # Calculate average trade duration
        completed_trades = [t for t in self.trade_history.values() if t.is_completed]
        if completed_trades:
            metrics.avg_trade_duration = sum(t.duration for t in completed_trades) / len(completed_trades)
        
        # Add to history
        self.metrics_history.append(metrics)
        
        # Return the collected metrics
        return metrics
    
    def track_signal(self, signal: TradeSignal, metrics: Optional[Dict[str, Any]] = None) -> SignalMetrics:
        """
        Track a trading signal.
        
        Args:
            signal: The trade signal to track
            metrics: Additional metrics about the signal
            
        Returns:
            SignalMetrics object
        """
        # Create signal metrics
        signal_metrics = SignalMetrics(
            signal_id=signal.signal_id,
            strategy_name=self.strategy_name,
            signal_type=signal.signal_type.name,
            symbol=signal.symbol,
            timestamp=signal.timestamp,
            metadata=signal.metadata or {}
        )
        
        # Add additional metrics if provided
        if metrics:
            for key, value in metrics.items():
                if hasattr(signal_metrics, key):
                    setattr(signal_metrics, key, value)
                else:
                    signal_metrics.metadata[key] = value
        
        # Update metadata with expected profit if available
        if 'expected_profit' in signal.metadata:
            signal_metrics.expected_profit = signal.metadata['expected_profit']
        
        # Update metadata with utility score if available
        if 'utility_score' in signal.metadata:
            signal_metrics.utility_score = signal.metadata['utility_score']
        
        # Check if the signal was sized by a risk manager
        if 'position_sizing' in signal.metadata and signal.metadata['position_sizing'].get('enhanced', False):
            signal_metrics.sized_position = True
        
        # Update counter and store signal metrics
        self.total_signals_generated += 1
        self.signal_history[signal.signal_id] = signal_metrics
        self.pending_signals[signal.signal_id] = signal_metrics
        
        return signal_metrics
    
    def track_signal_execution(self, signal_id: str, executed: bool, latency: float = 0.0, reason: str = ""):
        """
        Track the execution of a signal.
        
        Args:
            signal_id: ID of the signal
            executed: Whether the signal was executed successfully
            latency: Time from signal generation to execution in ms
            reason: Reason for rejection if not executed
        """
        if signal_id in self.pending_signals:
            signal_metrics = self.pending_signals[signal_id]
            
            # Update execution status and latency
            signal_metrics.execution_status = "EXECUTED" if executed else "REJECTED"
            signal_metrics.execution_latency = latency
            
            # Add rejection reason if provided
            if not executed and reason:
                signal_metrics.metadata['rejection_reason'] = reason
            
            # Update executed signals counter
            if executed:
                self.total_signals_executed += 1
            
            # Remove from pending signals
            del self.pending_signals[signal_id]
    
    def track_trade(self, trade_id: str, symbol: str, exchange: str, direction: str, 
                   size: float, entry_price: float, entry_time: datetime,
                   signal_id: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> TradeMetrics:
        """
        Track a new trade.
        
        Args:
            trade_id: Unique identifier for the trade
            symbol: Trading symbol
            exchange: Exchange where the trade was executed
            direction: "LONG" or "SHORT"
            size: Trade size
            entry_price: Entry price
            entry_time: Entry timestamp
            signal_id: ID of the signal that generated this trade
            metadata: Additional trade metadata
            
        Returns:
            TradeMetrics object
        """
        # Create trade metrics
        trade_metrics = TradeMetrics(
            trade_id=trade_id,
            strategy_name=self.strategy_name,
            symbol=symbol,
            exchange=exchange,
            direction=direction,
            size=size,
            entry_price=entry_price,
            entry_time=entry_time,
            metadata=metadata or {}
        )
        
        # Link to signal if provided
        if signal_id:
            trade_metrics.metadata['signal_id'] = signal_id
        
        # Update counter and store trade metrics
        self.total_trades_executed += 1
        self.trade_history[trade_id] = trade_metrics
        self.current_trades[trade_id] = trade_metrics
        
        return trade_metrics
    
    def track_trade_update(self, trade_id: str, current_price: float, 
                          unrealized_pnl: float, tags: Optional[List[str]] = None):
        """
        Update tracking for an open trade.
        
        Args:
            trade_id: Unique identifier for the trade
            current_price: Current price of the asset
            unrealized_pnl: Current unrealized PnL
            tags: Tags to add to the trade
        """
        if trade_id in self.current_trades:
            trade = self.current_trades[trade_id]
            
            # Update PnL
            trade.pnl = unrealized_pnl
            
            # Calculate PnL percentage
            trade.pnl_percentage = (unrealized_pnl / (trade.entry_price * trade.size)) * 100
            
            # Add tags if provided
            if tags:
                for tag in tags:
                    if tag not in trade.tags:
                        trade.tags.append(tag)
    
    def track_trade_exit(self, trade_id: str, exit_price: float, exit_time: datetime, 
                        realized_pnl: float, fees: float = 0.0, slippage: float = 0.0):
        """
        Track the exit of a trade.
        
        Args:
            trade_id: Unique identifier for the trade
            exit_price: Exit price
            exit_time: Exit timestamp
            realized_pnl: Realized PnL
            fees: Trading fees
            slippage: Price slippage
        """
        if trade_id in self.current_trades:
            trade = self.current_trades[trade_id]
            
            # Update trade metrics
            trade.exit_price = exit_price
            trade.exit_time = exit_time
            trade.pnl = realized_pnl
            trade.fees = fees
            trade.slippage = slippage
            trade.is_completed = True
            
            # Calculate duration
            if trade.entry_time and exit_time:
                duration = (exit_time - trade.entry_time).total_seconds()
                trade.duration = max(0, duration)  # Ensure non-negative
            
            # Calculate PnL percentage
            if trade.entry_price > 0 and trade.size > 0:
                trade.pnl_percentage = (realized_pnl / (trade.entry_price * trade.size)) * 100
            
            # Update win/loss counters
            if realized_pnl > 0:
                self.winning_trades += 1
            else:
                self.losing_trades += 1
            
            # Update total PnL
            self.total_pnl += realized_pnl
            
            # Remove from current trades
            del self.current_trades[trade_id]
    
    def track_opportunity(self, opportunity: ArbitrageOpportunity) -> str:
        """
        Track an arbitrage opportunity.
        
        Args:
            opportunity: ArbitrageOpportunity object
            
        Returns:
            Opportunity ID for reference
        """
        opportunity_id = str(id(opportunity))
        self.opportunity_history[opportunity_id] = opportunity
        return opportunity_id
    
    def track_sized_opportunity(self, opportunity_id: str, sized_opportunity: SizedOpportunity):
        """
        Track a sized arbitrage opportunity.
        
        Args:
            opportunity_id: ID of the original opportunity
            sized_opportunity: SizedOpportunity object
        """
        self.sized_opportunity_history[opportunity_id] = sized_opportunity
    
    def track_position(self, symbol: str, exchange: str, size: float, 
                      average_price: float, unrealized_pnl: float):
        """
        Track an open position.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange where the position is held
            size: Position size (negative for short positions)
            average_price: Average entry price
            unrealized_pnl: Current unrealized PnL
        """
        # Implementation will depend on the position tracking system
        pass
    
    def persist_metrics(self):
        """Persist metrics to storage."""
        if not self.persistence_enabled:
            return
        
        try:
            # Implement persistence logic (e.g., to TimescaleDB, InfluxDB, files)
            # For simplicity, we'll just log the metrics
            logger.debug(f"Persisting metrics for {self.strategy_name}")
        except Exception as e:
            logger.error(f"Error persisting metrics: {e}", exc_info=True)
    
    def _prune_history(self):
        """Prune history to prevent memory issues."""
        if len(self.metrics_history) > self.max_history_length:
            self.metrics_history = self.metrics_history[-self.max_history_length:]
        
        # Prune other histories as needed
        if len(self.signal_history) > self.max_history_length:
            oldest_signals = sorted(self.signal_history.keys(), 
                                  key=lambda x: self.signal_history[x].timestamp)[:len(self.signal_history) - self.max_history_length]
            for signal_id in oldest_signals:
                del self.signal_history[signal_id]
        
        if len(self.trade_history) > self.max_history_length:
            oldest_trades = sorted(self.trade_history.keys(), 
                                 key=lambda x: self.trade_history[x].entry_time)[:len(self.trade_history) - self.max_history_length]
            for trade_id in oldest_trades:
                del self.trade_history[trade_id]
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Get a summary of performance metrics.
        
        Returns:
            Dictionary with summarized performance metrics
        """
        # Calculate win rate
        total_completed_trades = self.winning_trades + self.losing_trades
        win_rate = (self.winning_trades / total_completed_trades * 100) if total_completed_trades > 0 else 0
        
        # Calculate signal execution rate
        signal_execution_rate = (self.total_signals_executed / self.total_signals_generated * 100) if self.total_signals_generated > 0 else 0
        
        # Return summary
        return {
            'strategy_name': self.strategy_name,
            'total_pnl': self.total_pnl,
            'signals_generated': self.total_signals_generated,
            'signals_executed': self.total_signals_executed,
            'signal_execution_rate': signal_execution_rate,
            'trades_executed': self.total_trades_executed,
            'winning_trades': self.winning_trades,
            'losing_trades': self.losing_trades,
            'win_rate': win_rate,
            'current_open_trades': len(self.current_trades),
            'pending_signals': len(self.pending_signals)
        }
    
    def get_metrics_dataframe(self) -> pd.DataFrame:
        """
        Get metrics history as a DataFrame.
        
        Returns:
            DataFrame with metrics history
        """
        return pd.DataFrame([asdict(m) for m in self.metrics_history])
    
    def get_trades_dataframe(self, completed_only: bool = False) -> pd.DataFrame:
        """
        Get trade history as a DataFrame.
        
        Args:
            completed_only: If True, include only completed trades
            
        Returns:
            DataFrame with trade history
        """
        trades = self.trade_history.values()
        if completed_only:
            trades = [t for t in trades if t.is_completed]
        
        return pd.DataFrame([asdict(t) for t in trades])
    
    def get_signals_dataframe(self) -> pd.DataFrame:
        """
        Get signal history as a DataFrame.
        
        Returns:
            DataFrame with signal history
        """
        return pd.DataFrame([asdict(s) for s in self.signal_history.values()])


class PerformanceMonitor:
    """
    System for monitoring performance across multiple strategies.
    
    This class aggregates data from individual PerformanceTrackers and
    provides portfolio-level monitoring and alerting.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the performance monitor.
        
        Args:
            config: Configuration parameters
        """
        self.config = config or {}
        self.trackers: Dict[str, PerformanceTracker] = {}
        self.alerts: List[Dict[str, Any]] = []
        
        # Alert thresholds
        self.alert_thresholds = {
            'drawdown_threshold': self.config.get('drawdown_threshold', 10.0),  # percentage
            'pnl_threshold': self.config.get('pnl_threshold', -100.0),  # absolute value
            'win_rate_threshold': self.config.get('win_rate_threshold', 30.0)  # percentage
        }
    
    def register_tracker(self, tracker: PerformanceTracker):
        """
        Register a strategy performance tracker.
        
        Args:
            tracker: PerformanceTracker instance
        """
        self.trackers[tracker.strategy_name] = tracker
    
    def get_tracker(self, strategy_name: str) -> Optional[PerformanceTracker]:
        """
        Get a tracker by strategy name.
        
        Args:
            strategy_name: Name of the strategy
            
        Returns:
            PerformanceTracker instance or None if not found
        """
        return self.trackers.get(strategy_name)
    
    def get_portfolio_metrics(self) -> Dict[str, Any]:
        """
        Get aggregated metrics across all strategies.
        
        Returns:
            Dictionary with portfolio-level metrics
        """
        # Initialize aggregated metrics
        portfolio_metrics = {
            'total_pnl': 0.0,
            'total_signals_generated': 0,
            'total_signals_executed': 0,
            'total_trades_executed': 0,
            'total_winning_trades': 0,
            'total_losing_trades': 0,
            'active_strategies': len(self.trackers),
            'strategies_with_open_trades': 0
        }
        
        # Aggregate metrics from all trackers
        for tracker in self.trackers.values():
            summary = tracker.get_performance_summary()
            
            portfolio_metrics['total_pnl'] += summary['total_pnl']
            portfolio_metrics['total_signals_generated'] += summary['signals_generated']
            portfolio_metrics['total_signals_executed'] += summary['signals_executed']
            portfolio_metrics['total_trades_executed'] += summary['trades_executed']
            portfolio_metrics['total_winning_trades'] += summary['winning_trades']
            portfolio_metrics['total_losing_trades'] += summary['losing_trades']
            
            if summary['current_open_trades'] > 0:
                portfolio_metrics['strategies_with_open_trades'] += 1
        
        # Calculate portfolio-level rates
        total_completed_trades = portfolio_metrics['total_winning_trades'] + portfolio_metrics['total_losing_trades']
        if total_completed_trades > 0:
            portfolio_metrics['portfolio_win_rate'] = (portfolio_metrics['total_winning_trades'] / total_completed_trades) * 100
        else:
            portfolio_metrics['portfolio_win_rate'] = 0
        
        if portfolio_metrics['total_signals_generated'] > 0:
            portfolio_metrics['portfolio_execution_rate'] = (portfolio_metrics['total_signals_executed'] / portfolio_metrics['total_signals_generated']) * 100
        else:
            portfolio_metrics['portfolio_execution_rate'] = 0
        
        return portfolio_metrics
    
    def check_alerts(self) -> List[Dict[str, Any]]:
        """
        Check for alert conditions.
        
        Returns:
            List of triggered alerts
        """
        new_alerts = []
        
        # Check alerts for each strategy
        for strategy_name, tracker in self.trackers.items():
            summary = tracker.get_performance_summary()
            
            # Check drawdown alert
            latest_metrics = tracker.metrics_history[-1] if tracker.metrics_history else None
            if latest_metrics and latest_metrics.current_drawdown > self.alert_thresholds['drawdown_threshold']:
                new_alerts.append({
                    'timestamp': datetime.now(),
                    'strategy': strategy_name,
                    'type': 'DRAWDOWN_ALERT',
                    'message': f"Drawdown of {latest_metrics.current_drawdown:.2f}% exceeds threshold of {self.alert_thresholds['drawdown_threshold']}%",
                    'level': 'WARNING',
                    'value': latest_metrics.current_drawdown
                })
            
            # Check PnL alert
            if summary['total_pnl'] < self.alert_thresholds['pnl_threshold']:
                new_alerts.append({
                    'timestamp': datetime.now(),
                    'strategy': strategy_name,
                    'type': 'PNL_ALERT',
                    'message': f"PnL of ${summary['total_pnl']:.2f} is below threshold of ${self.alert_thresholds['pnl_threshold']:.2f}",
                    'level': 'WARNING',
                    'value': summary['total_pnl']
                })
            
            # Check win rate alert
            if summary['win_rate'] < self.alert_thresholds['win_rate_threshold'] and total_completed_trades > 10:
                total_completed_trades = summary['winning_trades'] + summary['losing_trades']
                new_alerts.append({
                    'timestamp': datetime.now(),
                    'strategy': strategy_name,
                    'type': 'WIN_RATE_ALERT',
                    'message': f"Win rate of {summary['win_rate']:.2f}% is below threshold of {self.alert_thresholds['win_rate_threshold']}%",
                    'level': 'WARNING',
                    'value': summary['win_rate']
                })
        
        # Add new alerts to history
        self.alerts.extend(new_alerts)
        
        return new_alerts
    
    def send_alerts(self, alerts: List[Dict[str, Any]]):
        """
        Send alerts to configured channels.
        
        Args:
            alerts: List of alerts to send
        """
        # Implement alert sending logic (e.g., email, Slack, Telegram)
        for alert in alerts:
            logger.warning(f"ALERT: {alert['type']} - {alert['message']}")
    
    async def run_monitoring_loop(self, interval: int = 60):
        """
        Run the monitoring loop.
        
        Args:
            interval: Monitoring interval in seconds
        """
        while True:
            try:
                # Check for alerts
                new_alerts = self.check_alerts()
                
                # Send alerts if any
                if new_alerts:
                    self.send_alerts(new_alerts)
                
                # Wait for the next check
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}", exc_info=True)
                await asyncio.sleep(10)  # Wait a bit before retrying 