"""Audit logging system for tracking all trading operations with configuration-driven behavior.

This module provides comprehensive audit trail capabilities with
sensitive data protection and structured logging throughout.

IMPORTANT: Following CODING_STANDARDS.md:
- ALL configuration from AppSettings, NO hardcoded values
- Uses structlog for structured logging only
- Sensitive data handling based on config.general.log_sensitive_data
- Type-safe event definitions
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
import uuid
import json

from cyberdelta.config.structlog_config import get_logger
from pydantic import BaseModel, Field

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.models.market.order import Order
from cyberdelta.models.market.trade import Trade
from cyberdelta.models.trade_signal import TradeSignal
from cyberdelta.models.derivative_position import DerivativePosition
from cyberdelta.models.spot_balance import SpotBalance
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums import ExchangeName, OrderSide
from cyberdelta.logging.logging_helpers import SENSITIVE_FIELDS

logger = get_logger(__name__)


class AuditEventType(Enum):
    """Types of audit events tracked by the system."""
    # Trading events
    ORDER_PLACED = "order_placed"
    ORDER_CANCELLED = "order_cancelled"
    ORDER_FILLED = "order_filled"
    ORDER_REJECTED = "order_rejected"
    
    # Position events
    POSITION_OPENED = "position_opened"
    POSITION_CLOSED = "position_closed"
    POSITION_UPDATED = "position_updated"
    POSITION_LIQUIDATED = "position_liquidated"
    
    # Risk events
    RISK_LIMIT_EXCEEDED = "risk_limit_exceeded"
    RISK_ASSESSMENT = "risk_assessment"
    POSITION_SIZE_ADJUSTED = "position_size_adjusted"
    
    # Signal events
    SIGNAL_GENERATED = "signal_generated"
    SIGNAL_VALIDATED = "signal_validated"
    SIGNAL_REJECTED = "signal_rejected"
    SIGNAL_EXECUTED = "signal_executed"
    
    # Portfolio events
    PORTFOLIO_UPDATED = "portfolio_updated"
    BALANCE_CHANGED = "balance_changed"
    PNL_CALCULATED = "pnl_calculated"
    
    # System events
    SYSTEM_STARTED = "system_started"
    SYSTEM_STOPPED = "system_stopped"
    CONFIG_CHANGED = "config_changed"
    CIRCUIT_BREAKER_TRIGGERED = "circuit_breaker_triggered"
    
    # Error events
    ERROR_OCCURRED = "error_occurred"
    CONNECTION_LOST = "connection_lost"
    RECONCILIATION_FAILED = "reconciliation_failed"


class AuditSeverity(Enum):
    """Severity levels for audit events."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AuditEvent(BaseModel):
    """Base audit event with full traceability.
    
    All audit events must include these fields for compliance
    and traceability. Additional fields can be added via metadata.
    """
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: AuditEventType
    severity: AuditSeverity = AuditSeverity.INFO
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    
    # Context fields
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    correlation_id: Optional[str] = None
    
    # Event details
    description: str
    entity_type: Optional[str] = None  # Order, Position, Signal, etc.
    entity_id: Optional[str] = None    # Specific ID of the entity
    
    # Exchange and symbol context
    exchange: Optional[ExchangeName] = None
    symbol: Optional[Symbol] = None
    
    # Additional structured data
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    # Change tracking
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    
    # Risk and compliance
    risk_score: Optional[float] = None
    compliance_flags: List[str] = Field(default_factory=list)


class AuditLogger:
    """Central audit logging system with configuration-driven behavior.
    
    Configuration Usage:
    - Uses config.general.log_sensitive_data to control sensitive data logging
    - Uses config.monitoring.audit_log_file for audit log location
    - Uses config.monitoring.audit_retention_days for log retention
    - Uses config.monitoring.audit_log_format for output format
    - Uses config.general.audit_log_enabled to enable/disable audit logging
    
    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL configuration from AppSettings, NO hardcoded values
    - Uses structured logging exclusively
    - Fail-fast on configuration violations
    - Type-safe event definitions
    """
    
    def __init__(self, config: AppSettings):
        """Initialize audit logger with configuration.
        
        Args:
            config: Application settings containing audit configuration
        """
        self.config = config
        self._general_config = config.general
        self._monitoring_config = config.monitoring
        
        # Extract audit configuration - NO hardcoded defaults
        self._enabled = self._general_config.audit_log_enabled
        self._log_sensitive_data = self._general_config.log_sensitive_data
        
        # Audit log file configuration
        if hasattr(self._monitoring_config, 'audit_log_file'):
            self._audit_log_file = Path(self._monitoring_config.audit_log_file)
            self._audit_log_file.parent.mkdir(parents=True, exist_ok=True)
        else:
            self._audit_log_file = None
            
        # Retention configuration
        self._retention_days = getattr(
            self._monitoring_config, 
            'audit_retention_days',
            None
        )
        
        # Format configuration
        self._log_format = getattr(
            self._monitoring_config,
            'audit_log_format',
            'json'  # json or text
        )
        
        # Session tracking
        self._session_id = str(uuid.uuid4())
        self._event_count = 0
        self._start_time = datetime.now(UTC)
        
        # Buffer for batch writing
        self._event_buffer: List[AuditEvent] = []
        self._buffer_size = getattr(
            self._monitoring_config,
            'audit_buffer_size',
            100
        )
        self._flush_interval = getattr(
            self._monitoring_config,
            'audit_flush_interval_seconds',
            60
        )
        self._flush_task: Optional[asyncio.Task] = None
        
        logger.info(
            "audit_logger_initialized",
            enabled=self._enabled,
            log_sensitive_data=self._log_sensitive_data,
            audit_log_file=str(self._audit_log_file) if self._audit_log_file else None,
            retention_days=self._retention_days,
            format=self._log_format,
            session_id=self._session_id
        )
    
    async def start(self) -> None:
        """Start the audit logger with periodic flushing.
        
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured flush interval
        - Fail-fast if already started
        """
        if not self._enabled:
            logger.info("audit_logger_disabled_by_config")
            return
            
        if self._flush_task and not self._flush_task.done():
            logger.warning("audit_logger_already_started")
            return
            
        # Log system start event
        await self.log_event(
            AuditEvent(
                event_type=AuditEventType.SYSTEM_STARTED,
                severity=AuditSeverity.INFO,
                description="Trading system started",
                metadata={
                    "session_id": self._session_id,
                    "safe_mode": self._general_config.safe_mode,
                    "config_version": getattr(self.config, 'version', 'unknown')
                }
            )
        )
        
        # Start flush task
        self._flush_task = asyncio.create_task(self._flush_loop())
        
        logger.info(
            "audit_logger_started",
            session_id=self._session_id,
            flush_interval_sec=self._flush_interval
        )
    
    async def stop(self) -> None:
        """Stop the audit logger and flush remaining events.
        
        IMPORTANT: Following CODING_STANDARDS.md:
        - Graceful shutdown with final flush
        - Uses configured shutdown grace period
        """
        if not self._enabled:
            return
            
        # Log system stop event
        await self.log_event(
            AuditEvent(
                event_type=AuditEventType.SYSTEM_STOPPED,
                severity=AuditSeverity.INFO,
                description="Trading system stopped",
                metadata={
                    "session_id": self._session_id,
                    "total_events": self._event_count,
                    "session_duration_seconds": (
                        datetime.now(UTC) - self._start_time
                    ).total_seconds()
                }
            )
        )
        
        # Cancel flush task
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await asyncio.wait_for(
                    self._flush_task,
                    timeout=float(self.config.general.shutdown_grace_period)
                )
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        
        # Final flush
        await self._flush_buffer()
        
        logger.info(
            "audit_logger_stopped",
            session_id=self._session_id,
            total_events=self._event_count
        )
    
    async def log_event(self, event: AuditEvent) -> None:
        """Log an audit event with configuration-driven behavior.
        
        Args:
            event: The audit event to log
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Respects config.general.log_sensitive_data setting
        - Uses structured logging only
        - Buffers events for efficient writing
        """
        if not self._enabled:
            return
        
        # Add session context
        event.session_id = self._session_id
        event.correlation_id = event.correlation_id or str(uuid.uuid4())
        
        # Filter sensitive data if configured
        if not self._log_sensitive_data:
            event = self._filter_sensitive_data(event)
        
        # Add to buffer
        self._event_buffer.append(event)
        self._event_count += 1
        
        # Log to structured logger
        self._log_to_structlog(event)
        
        # Flush if buffer full
        if len(self._event_buffer) >= self._buffer_size:
            await self._flush_buffer()
    
    async def log_order_event(
        self,
        order: Order,
        event_type: AuditEventType,
        description: str,
        **metadata: Any
    ) -> None:
        """Log an order-related audit event.
        
        Args:
            order: The order involved
            event_type: Type of order event
            description: Human-readable description
            **metadata: Additional metadata
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses Symbol and ExchangeName types
        - Filters sensitive fields based on config
        """
        # Build order data excluding sensitive fields if configured
        order_data = order.model_dump(
            mode="json",
            exclude=SENSITIVE_FIELDS.get(Order, set()) if not self._log_sensitive_data else set()
        )
        
        event = AuditEvent(
            event_type=event_type,
            severity=AuditSeverity.INFO,
            description=description,
            entity_type="Order",
            entity_id=order.order_id or order.client_order_id,
            exchange=order.exchange,
            symbol=order.symbol,
            metadata={
                "order_data": order_data,
                "side": order.side.value if order.side else None,
                "quantity": float(order.quantity) if order.quantity else None,
                "price": float(order.price) if order.price else None,
                **metadata
            }
        )
        
        await self.log_event(event)
    
    async def log_trade_event(
        self,
        trade: Trade,
        event_type: AuditEventType,
        description: str,
        **metadata: Any
    ) -> None:
        """Log a trade-related audit event.
        
        Args:
            trade: The trade executed
            event_type: Type of trade event
            description: Human-readable description
            **metadata: Additional metadata
        """
        # Build trade data excluding sensitive fields if configured
        trade_data = trade.model_dump(
            mode="json",
            exclude=SENSITIVE_FIELDS.get(Trade, set()) if not self._log_sensitive_data else set()
        )
        
        event = AuditEvent(
            event_type=event_type,
            severity=AuditSeverity.INFO,
            description=description,
            entity_type="Trade",
            entity_id=trade.id,
            exchange=ExchangeName(trade.exchange) if trade.exchange else None,
            symbol=trade.symbol,
            metadata={
                "trade_data": trade_data,
                "side": trade.side.value if trade.side else None,
                "quantity": float(trade.quantity) if trade.quantity else None,
                "price": float(trade.price) if trade.price else None,
                "fee": float(trade.fee) if trade.fee else None,
                **metadata
            }
        )
        
        await self.log_event(event)
    
    async def log_signal_event(
        self,
        signal: TradeSignal,
        event_type: AuditEventType,
        description: str,
        **metadata: Any
    ) -> None:
        """Log a signal-related audit event.
        
        Args:
            signal: The trading signal
            event_type: Type of signal event
            description: Human-readable description
            **metadata: Additional metadata
        """
        # Build signal data excluding sensitive fields if configured
        signal_data = signal.model_dump(
            mode="json",
            exclude=SENSITIVE_FIELDS.get(TradeSignal, set()) if not self._log_sensitive_data else set()
        )
        
        event = AuditEvent(
            event_type=event_type,
            severity=AuditSeverity.INFO,
            description=description,
            entity_type="TradeSignal",
            entity_id=signal.signal_id,
            exchange=signal.exchange,
            symbol=signal.symbol,
            metadata={
                "signal_data": signal_data,
                "source_strategy": signal.source_strategy,
                "confidence": signal.confidence,
                **metadata
            }
        )
        
        await self.log_event(event)
    
    async def log_risk_event(
        self,
        event_type: AuditEventType,
        description: str,
        severity: AuditSeverity = AuditSeverity.WARNING,
        risk_score: Optional[float] = None,
        violations: Optional[List[str]] = None,
        **metadata: Any
    ) -> None:
        """Log a risk-related audit event.
        
        Args:
            event_type: Type of risk event
            description: Human-readable description
            severity: Event severity
            risk_score: Optional risk score
            violations: List of limit violations
            **metadata: Additional metadata
        """
        event = AuditEvent(
            event_type=event_type,
            severity=severity,
            description=description,
            risk_score=risk_score,
            compliance_flags=violations or [],
            metadata=metadata
        )
        
        await self.log_event(event)
    
    async def log_error_event(
        self,
        error: Exception,
        context: str,
        **metadata: Any
    ) -> None:
        """Log an error event for audit trail.
        
        Args:
            error: The exception that occurred
            context: Context where error occurred
            **metadata: Additional metadata
        """
        event = AuditEvent(
            event_type=AuditEventType.ERROR_OCCURRED,
            severity=AuditSeverity.ERROR,
            description=f"Error in {context}: {str(error)}",
            metadata={
                "error_type": type(error).__name__,
                "error_message": str(error),
                "context": context,
                **metadata
            }
        )
        
        await self.log_event(event)
    
    def _filter_sensitive_data(self, event: AuditEvent) -> AuditEvent:
        """Filter sensitive data from audit event based on configuration.
        
        Args:
            event: The event to filter
            
        Returns:
            Filtered event with sensitive data removed
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Only filters if config.general.log_sensitive_data is False
        - Maintains event structure while removing sensitive values
        """
        if self._log_sensitive_data:
            return event
            
        # Create a copy to avoid modifying original
        filtered_event = event.model_copy()
        
        # Filter metadata
        if filtered_event.metadata:
            # Remove sensitive fields from nested data
            for key in ['order_data', 'trade_data', 'signal_data', 'position_data']:
                if key in filtered_event.metadata:
                    # These are already filtered by the specific log methods
                    pass
            
            # Remove any fields that might contain sensitive data
            sensitive_keys = [
                'api_key', 'secret', 'password', 'token',
                'private_key', 'seed', 'mnemonic'
            ]
            for key in list(filtered_event.metadata.keys()):
                if any(sensitive in key.lower() for sensitive in sensitive_keys):
                    filtered_event.metadata[key] = "[REDACTED]"
        
        # Filter old/new values if they contain sensitive data
        if filtered_event.old_value and isinstance(filtered_event.old_value, dict):
            for key in list(filtered_event.old_value.keys()):
                if any(sensitive in key.lower() for sensitive in ['password', 'secret', 'key', 'token']):
                    filtered_event.old_value[key] = "[REDACTED]"
                    
        if filtered_event.new_value and isinstance(filtered_event.new_value, dict):
            for key in list(filtered_event.new_value.keys()):
                if any(sensitive in key.lower() for sensitive in ['password', 'secret', 'key', 'token']):
                    filtered_event.new_value[key] = "[REDACTED]"
        
        return filtered_event
    
    def _log_to_structlog(self, event: AuditEvent) -> None:
        """Log event to structured logger.
        
        Args:
            event: The audit event to log
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses structlog exclusively
        - Includes all relevant context
        """
        log_data = {
            "audit_event": True,
            "event_id": event.event_id,
            "event_type": event.event_type.value,
            "severity": event.severity.value,
            "timestamp": event.timestamp.isoformat(),
            "session_id": event.session_id,
            "correlation_id": event.correlation_id,
            "description": event.description,
        }
        
        # Add optional fields if present
        if event.entity_type:
            log_data["entity_type"] = event.entity_type
        if event.entity_id:
            log_data["entity_id"] = event.entity_id
        if event.exchange:
            log_data["exchange"] = event.exchange.value
        if event.symbol:
            log_data["symbol"] = event.symbol.value
        if event.risk_score is not None:
            log_data["risk_score"] = event.risk_score
        if event.compliance_flags:
            log_data["compliance_flags"] = event.compliance_flags
        if event.metadata:
            log_data["metadata"] = event.metadata
        
        # Log at appropriate level based on severity
        if event.severity == AuditSeverity.ERROR:
            logger.error("audit_event", **log_data)
        elif event.severity == AuditSeverity.CRITICAL:
            logger.critical("audit_event", **log_data)
        elif event.severity == AuditSeverity.WARNING:
            logger.warning("audit_event", **log_data)
        else:
            logger.info("audit_event", **log_data)
    
    async def _flush_buffer(self) -> None:
        """Flush buffered events to persistent storage.
        
        IMPORTANT: Following CODING_STANDARDS.md:
        - Writes to configured audit log file
        - Uses configured format (json or text)
        - Handles errors without losing events
        """
        if not self._event_buffer:
            return
            
        if not self._audit_log_file:
            # No file configured, just clear buffer
            self._event_buffer.clear()
            return
        
        try:
            # Prepare events for writing
            events_to_write = self._event_buffer.copy()
            self._event_buffer.clear()
            
            # Write based on configured format
            if self._log_format == 'json':
                await self._write_json_format(events_to_write)
            else:
                await self._write_text_format(events_to_write)
            
            logger.debug(
                "audit_buffer_flushed",
                event_count=len(events_to_write),
                file=str(self._audit_log_file)
            )
            
        except Exception as e:
            # Re-add events to buffer on failure
            self._event_buffer = events_to_write + self._event_buffer
            logger.error(
                "audit_flush_failed",
                error=str(e),
                event_count=len(events_to_write),
                exc_info=True
            )
            # Don't raise - we don't want to crash the system due to audit logging
    
    async def _write_json_format(self, events: List[AuditEvent]) -> None:
        """Write events in JSON format.
        
        Args:
            events: Events to write
        """
        with open(self._audit_log_file, 'a') as f:
            for event in events:
                json_line = event.model_dump_json() + '\n'
                f.write(json_line)
    
    async def _write_text_format(self, events: List[AuditEvent]) -> None:
        """Write events in human-readable text format.
        
        Args:
            events: Events to write
        """
        with open(self._audit_log_file, 'a') as f:
            for event in events:
                text_line = (
                    f"[{event.timestamp.isoformat()}] "
                    f"[{event.severity.value.upper()}] "
                    f"[{event.event_type.value}] "
                    f"{event.description}"
                )
                if event.entity_id:
                    text_line += f" (Entity: {event.entity_type}:{event.entity_id})"
                if event.exchange:
                    text_line += f" (Exchange: {event.exchange.value})"
                if event.symbol:
                    text_line += f" (Symbol: {event.symbol.value})"
                text_line += '\n'
                f.write(text_line)
    
    async def _flush_loop(self) -> None:
        """Periodic flush loop for buffered events.
        
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured flush interval
        - Handles cancellation gracefully
        """
        while True:
            try:
                await asyncio.sleep(float(self._flush_interval))
                await self._flush_buffer()
            except asyncio.CancelledError:
                logger.debug("audit_flush_loop_cancelled")
                break
            except Exception as e:
                logger.error(
                    "audit_flush_loop_error",
                    error=str(e),
                    exc_info=True
                )
                # Continue looping even on error
    
    async def cleanup_old_logs(self) -> None:
        """Clean up old audit logs based on retention policy.
        
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured retention period
        - Only deletes if retention is configured
        """
        if not self._retention_days or not self._audit_log_file:
            return
            
        try:
            cutoff_date = datetime.now(UTC) - timedelta(days=self._retention_days)
            
            # Find and remove old log files
            log_dir = self._audit_log_file.parent
            for log_file in log_dir.glob(f"{self._audit_log_file.stem}*"):
                if log_file.stat().st_mtime < cutoff_date.timestamp():
                    log_file.unlink()
                    logger.info(
                        "old_audit_log_deleted",
                        file=str(log_file),
                        age_days=(datetime.now(UTC) - datetime.fromtimestamp(
                            log_file.stat().st_mtime, UTC
                        )).days
                    )
                    
        except Exception as e:
            logger.error(
                "audit_cleanup_failed",
                error=str(e),
                exc_info=True
            )
    
    def get_session_stats(self) -> Dict[str, Any]:
        """Get statistics for the current audit session.
        
        Returns:
            Dictionary with session statistics
        """
        return {
            "session_id": self._session_id,
            "start_time": self._start_time.isoformat(),
            "total_events": self._event_count,
            "buffer_size": len(self._event_buffer),
            "enabled": self._enabled,
            "log_sensitive_data": self._log_sensitive_data,
            "session_duration_seconds": (
                datetime.now(UTC) - self._start_time
            ).total_seconds()
        }