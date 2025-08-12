"""Monitoring-related enums."""

from enum import Enum


class ServiceType(Enum):
    """Types of services that can be monitored."""

    PORTFOLIO = "portfolio_service"
    MARKET_DATA = "market_data_service"
    RISK = "risk_service"
    EXECUTION = "execution_engine"
    TRADING = "trading_service"
    SIGNAL = "signal_service"
    STRATEGY = "strategy_service"
    EVENT_BUS = "event_bus"


class MetricType(Enum):
    """Types of metrics that can be collected."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


class AlertLevel(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertStatus(Enum):
    """Alert status tracking."""

    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


class AlertChannel(Enum):
    """Available alert channels."""

    LOG = "log"
    TELEGRAM = "telegram"


class AuditEventType(Enum):
    """Types of audit events."""

    ORDER_PLACED = "order_placed"
    ORDER_CANCELLED = "order_cancelled"
    ORDER_FILLED = "order_filled"
    ORDER_REJECTED = "order_rejected"
    POSITION_OPENED = "position_opened"
    POSITION_CLOSED = "position_closed"
    POSITION_MODIFIED = "position_modified"
    BALANCE_UPDATED = "balance_updated"
    RISK_LIMIT_BREACHED = "risk_limit_breached"
    RISK_LIMIT_EXCEEDED = "risk_limit_exceeded"
    STRATEGY_STARTED = "strategy_started"
    STRATEGY_STOPPED = "strategy_stopped"
    SYSTEM_STARTED = "system_started"
    SYSTEM_STOPPED = "system_stopped"
    ERROR_OCCURRED = "error_occurred"
    CONFIG_CHANGED = "config_changed"
    USER_ACTION = "user_action"
    SYSTEM_EVENT = "system_event"
    MARKET_EVENT = "market_event"
    PORTFOLIO_EVENT = "portfolio_event"
    RISK_EVENT = "risk_event"
    SIGNAL_EVENT = "signal_event"
    SIGNAL_GENERATED = "signal_generated"
    WORKFLOW_STARTED = "workflow_started"
    WORKFLOW_COMPLETED = "workflow_completed"
    WORKFLOW_FAILED = "workflow_failed"
    WORKFLOW_CANCELLED = "workflow_cancelled"
    EMERGENCY_STOP = "emergency_stop"
    CIRCUIT_BREAKER_OPENED = "circuit_breaker_opened"
    CIRCUIT_BREAKER_CLOSED = "circuit_breaker_closed"
    API_CALL = "api_call"
    DATABASE_OPERATION = "database_operation"
    FILE_OPERATION = "file_operation"
    NETWORK_OPERATION = "network_operation"
    VALIDATION_ERROR = "validation_error"
    AUTHENTICATION_EVENT = "authentication_event"
    AUTHORIZATION_EVENT = "authorization_event"
    COMPLIANCE_EVENT = "compliance_event"
    AUDIT_LOG_CREATED = "audit_log_created"


class AuditSeverity(Enum):
    """Severity levels for audit events."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class RiskType(Enum):
    """Types of risk management events.

    Used in RiskEvent structures for categorizing different
    types of risk violations and alerts.
    - LIMIT_BREACH: Position or exposure limit exceeded
    - DRAWDOWN: Portfolio drawdown threshold crossed
    - EXPOSURE: Total market exposure limit exceeded
    - MARGIN_CALL: Margin requirements not met
    """

    LIMIT_BREACH = "LIMIT_BREACH"
    DRAWDOWN = "DRAWDOWN"
    EXPOSURE = "EXPOSURE"
    MARGIN_CALL = "MARGIN_CALL"


class RiskSeverity(Enum):
    """Risk severity levels for escalation handling.

    Used in RiskEvent structures to determine response priority.
    - INFO: Informational risk metric update
    - WARNING: Risk threshold approached, monitoring required
    - CRITICAL: Risk limit exceeded, immediate action required
    - EMERGENCY: Critical system risk, emergency protocols triggered
    """

    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    EMERGENCY = "EMERGENCY"


class MarketDataType(Enum):
    """Types of market data events.

    Used in MarketData event structures for type-safe handling
    of different market data feeds.
    - TICK: Individual tick/price update
    - ORDERBOOK: Order book depth update
    - TRADE: Executed trade information
    - QUOTE: Best bid/ask quote update
    """

    TICK = "TICK"
    ORDERBOOK = "ORDERBOOK"
    TRADE = "TRADE"
    QUOTE = "QUOTE"


class BalanceEventType(Enum):
    """Types of balance change events.

    Used in BalanceEvent structures for tracking account
    balance lifecycle and lock status.
    - UPDATED: Balance amount changed
    - LOCKED: Funds locked for pending operation
    - UNLOCKED: Previously locked funds released
    - SETTLED: Final settlement of balance change
    """

    UPDATED = "UPDATED"
    LOCKED = "LOCKED"
    UNLOCKED = "UNLOCKED"
    SETTLED = "SETTLED"


class SystemEventType(Enum):
    """Types of system-level events.

    Used in SystemEvent structures for monitoring
    component lifecycle and health status.
    - STARTED: Component successfully started
    - STOPPED: Component cleanly stopped
    - ERROR: Component error occurred
    - WARNING: Component warning condition
    - HEALTH_CHECK: Periodic health status report
    """

    STARTED = "STARTED"
    STOPPED = "STOPPED"
    ERROR = "ERROR"
    WARNING = "WARNING"
    HEALTH_CHECK = "HEALTH_CHECK"


class HealthStatus(Enum):
    """System component health status levels.

    Used in SystemEvent and health monitoring for
    standardized component status reporting.
    - HEALTHY: Component operating normally
    - DEGRADED: Component functional with reduced performance
    - FAILED: Component non-functional, requires attention
    """

    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


class WorkflowStatus(Enum):
    """Workflow execution status levels.

    Used in workflow events and workflow context for
    standardized workflow status tracking.
    - PENDING: Workflow queued but not yet started
    - RUNNING: Workflow currently executing
    - COMPLETED: Workflow finished successfully
    - FAILED: Workflow encountered error and stopped
    - CANCELLED: Workflow was cancelled before completion
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
