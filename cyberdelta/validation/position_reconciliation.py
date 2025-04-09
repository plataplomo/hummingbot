"""
Position Reconciliation System for the CyberDeltaEngine.

This module provides validation between various position tracking systems to ensure consistency.
"""

import logging
from typing import Dict, List, Optional, Tuple, Any, Set
from datetime import datetime, timedelta
import numpy as np

from cyberdelta.core.models import Position
from cyberdelta.utils.config import Config

logger = logging.getLogger(__name__)


class PositionReconciliationSystem:
    """
    Validates and reconciles positions between different sources:
    1. Exchange API-reported positions
    2. Fill history-derived positions
    3. Local state tracking
    
    The system detects discrepancies, provides alerts, and optionally corrects the local state
    to match the authoritative source.
    """
    
    def __init__(self, config: Config, portfolio_tracker=None):
        """
        Initialize the position reconciliation system.
        
        Args:
            config: Application configuration
            portfolio_tracker: Reference to the portfolio tracker (optional, can be set later)
        """
        self.config = config
        self.portfolio_tracker = portfolio_tracker
        
        # Configuration parameters
        self.reconciliation_threshold = config.get(
            'validation.position_reconciliation.threshold', 0.05)  # 5% discrepancy threshold
        self.auto_correct = config.get(
            'validation.position_reconciliation.auto_correct', False)
        self.check_interval = config.get(
            'validation.position_reconciliation.check_interval', 3600)  # seconds
        
        # Track the last reconciliation time
        self.last_check_time = datetime.now() - timedelta(seconds=self.check_interval + 1)
        
        # Record of discrepancies found
        self.discrepancy_history: List[Dict[str, Any]] = []
        
        # Recent results
        self.latest_results: Dict[str, Dict[str, Any]] = {}
        
    def register_portfolio_tracker(self, portfolio_tracker):
        """
        Register the portfolio tracker instance.
        
        Args:
            portfolio_tracker: The portfolio tracker instance
        """
        self.portfolio_tracker = portfolio_tracker
    
    async def check_positions(self, force: bool = False) -> Dict[str, Dict[str, Any]]:
        """
        Check for position discrepancies across all exchanges.
        
        Args:
            force: Force a check regardless of the interval
            
        Returns:
            Dictionary of reconciliation results by exchange
        """
        now = datetime.now()
        
        # Check if we should run reconciliation
        if not force and (now - self.last_check_time).total_seconds() < self.check_interval:
            logger.debug("Skipping position reconciliation, not due yet")
            return self.latest_results
        
        if self.portfolio_tracker is None:
            logger.error("Cannot reconcile positions: Portfolio tracker not registered")
            return {}
        
        logger.info("Running position reconciliation check")
        self.last_check_time = now
        
        # Get all active exchanges
        exchanges: List[str] = []
        for exchange_id in self.config.get('exchanges', {}).keys():
            if self.config.get(f'exchanges.{exchange_id}.enabled', False):
                exchanges.append(exchange_id)
        
        # Results by exchange
        results: Dict[str, Dict[str, Any]] = {}
        
        # Check each exchange
        for exchange in exchanges:
            api_client = self.portfolio_tracker.get_api_client(exchange)
            if not api_client:
                logger.warning(f"No API client registered for {exchange}, skipping reconciliation")
                continue
            
            # Get positions from all three sources
            try:
                # 1. Exchange API positions
                exchange_positions = await api_client.get_positions()
                
                # 2. Fill history-derived positions (from execution handler)
                execution_handler = self.portfolio_tracker.get_execution_handler(exchange)
                fill_positions = execution_handler.get_derived_positions() if execution_handler else []
                
                # 3. Local state tracking
                local_positions = self.portfolio_tracker.get_positions_by_exchange(exchange)
                
                # Perform the reconciliation
                exchange_results = self._reconcile_positions(
                    exchange, exchange_positions, fill_positions, local_positions)
                
                # Store the results
                results[exchange] = exchange_results
                
                # Record any discrepancies
                if exchange_results['discrepancies']:
                    self._record_discrepancy(exchange, exchange_results)
                    
                    # Auto-correct if enabled
                    if self.auto_correct:
                        self._apply_corrections(exchange, exchange_results)
                
            except Exception as e:
                logger.error(f"Error reconciling positions for {exchange}: {str(e)}")
                results[exchange] = {
                    'success': False,
                    'error': str(e),
                    'timestamp': now,
                    'discrepancies': []
                }
        
        self.latest_results = results
        return results
    
    def _reconcile_positions(
        self, 
        exchange: str,
        exchange_positions: List[Position],
        fill_positions: List[Position],
        local_positions: List[Position]
    ) -> Dict[str, Any]:
        """
        Reconcile positions from different sources for a given exchange.
        
        Args:
            exchange: Exchange identifier
            exchange_positions: Positions reported by exchange API
            fill_positions: Positions derived from fill history
            local_positions: Positions in the local state
            
        Returns:
            Reconciliation results
        """
        now = datetime.now()
        
        # Create position maps for easier comparison
        exchange_map = {p.symbol: p for p in exchange_positions}
        fill_map = {p.symbol: p for p in fill_positions}
        local_map = {p.symbol: p for p in local_positions}
        
        # Get all unique symbols
        all_symbols = set(exchange_map.keys()) | set(fill_map.keys()) | set(local_map.keys())
        
        # Check for discrepancies
        discrepancies = []
        
        for symbol in all_symbols:
            # Get positions from each source (or create empty position)
            exch_pos = exchange_map.get(symbol, Position(
                symbol=symbol, size=0, entry_price=0, mark_price=0, 
                liquidation_price=0, unrealized_pnl=0, leverage=0))
            
            fill_pos = fill_map.get(symbol, Position(
                symbol=symbol, size=0, entry_price=0, mark_price=0, 
                liquidation_price=0, unrealized_pnl=0, leverage=0))
            
            local_pos = local_map.get(symbol, Position(
                symbol=symbol, size=0, entry_price=0, mark_price=0, 
                liquidation_price=0, unrealized_pnl=0, leverage=0))
            
            # Calculate discrepancies
            exch_local_diff = abs(exch_pos.size - local_pos.size)
            fill_local_diff = abs(fill_pos.size - local_pos.size)
            exch_fill_diff = abs(exch_pos.size - fill_pos.size)
            
            # Calculate relative discrepancies (avoid division by zero)
            base_size = max(abs(exch_pos.size), abs(fill_pos.size), abs(local_pos.size))
            
            if base_size > 0:
                exch_local_pct = exch_local_diff / base_size
                fill_local_pct = fill_local_diff / base_size
                exch_fill_pct = exch_fill_diff / base_size
            else:
                # If all sizes are zero, there's no discrepancy
                exch_local_pct = fill_local_pct = exch_fill_pct = 0
            
            # Check if any discrepancy exceeds the threshold
            if (exch_local_pct > self.reconciliation_threshold or 
                fill_local_pct > self.reconciliation_threshold or 
                exch_fill_pct > self.reconciliation_threshold):
                
                # Determine the source of truth (exchange API is considered most authoritative)
                correct_size = exch_pos.size
                
                discrepancies.append({
                    'symbol': symbol,
                    'exchange_size': exch_pos.size,
                    'fill_size': fill_pos.size,
                    'local_size': local_pos.size,
                    'exchange_local_diff': exch_local_diff,
                    'exchange_local_pct': exch_local_pct,
                    'fill_local_diff': fill_local_diff,
                    'fill_local_pct': fill_local_pct,
                    'exchange_fill_diff': exch_fill_diff,
                    'exchange_fill_pct': exch_fill_pct,
                    'correct_size': correct_size,
                    'threshold_exceeded': True
                })
        
        return {
            'success': True,
            'timestamp': now,
            'discrepancies': discrepancies,
            'symbols_checked': len(all_symbols),
            'has_discrepancies': len(discrepancies) > 0
        }
    
    def _record_discrepancy(self, exchange: str, results: Dict[str, Any]):
        """
        Record a discrepancy for historical tracking.
        
        Args:
            exchange: Exchange identifier
            results: Reconciliation results
        """
        timestamp = results['timestamp']
        
        for discrepancy in results['discrepancies']:
            record = {
                'timestamp': timestamp,
                'exchange': exchange,
                'symbol': discrepancy['symbol'],
                'exchange_size': discrepancy['exchange_size'],
                'fill_size': discrepancy['fill_size'],
                'local_size': discrepancy['local_size'],
                'exchange_local_diff': discrepancy['exchange_local_diff'],
                'fill_local_diff': discrepancy['fill_local_diff'],
                'correct_size': discrepancy['correct_size'],
                'corrected': False
            }
            
            self.discrepancy_history.append(record)
            
            # Log the discrepancy
            logger.warning(
                f"Position discrepancy detected: {exchange} {discrepancy['symbol']} "
                f"[Exchange: {discrepancy['exchange_size']}, "
                f"Fill: {discrepancy['fill_size']}, "
                f"Local: {discrepancy['local_size']}]"
            )
    
    def _apply_corrections(self, exchange: str, results: Dict[str, Any]):
        """
        Apply corrections to the portfolio tracker based on reconciliation results.
        
        Args:
            exchange: Exchange identifier
            results: Reconciliation results
        """
        if not self.portfolio_tracker:
            logger.error("Cannot apply corrections: Portfolio tracker not registered")
            return
        
        for discrepancy in results['discrepancies']:
            symbol = discrepancy['symbol']
            correct_size = discrepancy['correct_size']
            
            # Get current position
            current_position = self.portfolio_tracker.get_position(exchange, symbol)
            
            if current_position is None:
                # Create a new position with correct size
                if correct_size != 0:
                    logger.info(f"Creating missing position: {exchange} {symbol} size={correct_size}")
                    
                    # Use exchange position data to create the position
                    exchange_position = next(
                        (p for p in self.portfolio_tracker._get_exchange_positions(exchange) 
                         if p.symbol == symbol),
                        None
                    )
                    
                    if exchange_position:
                        self.portfolio_tracker.update_position(exchange, exchange_position)
            else:
                # Update existing position with correct size
                if current_position.size != correct_size:
                    logger.info(
                        f"Correcting position: {exchange} {symbol} "
                        f"from {current_position.size} to {correct_size}"
                    )
                    
                    # Create updated position
                    updated_position = Position(
                        symbol=current_position.symbol,
                        size=correct_size,
                        entry_price=current_position.entry_price,
                        mark_price=current_position.mark_price,
                        liquidation_price=current_position.liquidation_price,
                        unrealized_pnl=current_position.unrealized_pnl,
                        leverage=current_position.leverage,
                        side=current_position.side
                    )
                    
                    self.portfolio_tracker.update_position(exchange, updated_position)
            
            # Mark as corrected in history
            for record in self.discrepancy_history:
                if (record['exchange'] == exchange and 
                    record['symbol'] == symbol and 
                    record['timestamp'] == results['timestamp']):
                    record['corrected'] = True
    
    def get_discrepancy_history(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Get the history of position discrepancies.
        
        Args:
            days: Number of days to include in history
            
        Returns:
            List of discrepancy records
        """
        cutoff_time = datetime.now() - timedelta(days=days)
        return [r for r in self.discrepancy_history if r['timestamp'] >= cutoff_time]
    
    def get_latest_results(self) -> Dict[str, Dict[str, Any]]:
        """
        Get the latest reconciliation results.
        
        Returns:
            Dictionary of reconciliation results by exchange
        """
        return self.latest_results
    
    def get_reconciliation_report(self) -> Dict[str, Any]:
        """
        Generate a summary report of position reconciliation.
        
        Returns:
            Report with summary statistics and recent discrepancies
        """
        now = datetime.now()
        recent_discrepancies = self.get_discrepancy_history(days=1)
        
        # Group discrepancies by exchange
        exchange_stats = {}
        for record in recent_discrepancies:
            exchange = record['exchange']
            if exchange not in exchange_stats:
                exchange_stats[exchange] = {
                    'total_discrepancies': 0,
                    'symbols_affected': set(),
                    'corrected': 0,
                    'uncorrected': 0
                }
            
            exchange_stats[exchange]['total_discrepancies'] += 1
            exchange_stats[exchange]['symbols_affected'].add(record['symbol'])
            
            if record['corrected']:
                exchange_stats[exchange]['corrected'] += 1
            else:
                exchange_stats[exchange]['uncorrected'] += 1
        
        # Convert sets to counts for serialization
        for exchange in exchange_stats:
            exchange_stats[exchange]['symbols_affected'] = len(
                exchange_stats[exchange]['symbols_affected'])
        
        return {
            'timestamp': now,
            'total_discrepancies_24h': len(recent_discrepancies),
            'exchange_stats': exchange_stats,
            'recent_discrepancies': recent_discrepancies[:10],  # Latest 10
            'last_check_time': self.last_check_time,
            'auto_correct_enabled': self.auto_correct,
            'reconciliation_threshold': self.reconciliation_threshold
        } 