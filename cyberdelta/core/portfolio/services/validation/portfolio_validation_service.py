"""Portfolio validation service with direct AppSettings access following risk module patterns."""

from __future__ import annotations

import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import DerivativePosition, Trade
from cyberdelta.core.portfolio.portfolio_types.portfolio_data_models import (
    ExchangeBalances,
    ExchangePositions,
    IssueDict,
    PortfolioMetrics,
    PortfolioState,
    ValidationStatistics,
)
from cyberdelta.core.portfolio.portfolio_types.validation_types import (
    ValidationCategory,
    ValidationChain,
    ValidationIssue,
    ValidationResult,
    ValidationSeverity,
    create_business_rule_violation,
    create_range_violation_issue,
)
from cyberdelta.enums.exchange_names import ExchangeName


if TYPE_CHECKING:
    from cyberdelta.core.models import SpotBalance
    from cyberdelta.core.portfolio.models.base import BaseStateModel
    from cyberdelta.core.portfolio.protocols import (
        StateContainerProtocol,
    )


class TradeValidator:
    """Validates trade data and business rules with direct AppSettings access."""

    # Symbol validation constants
    MIN_SYMBOL_LENGTH = 2  # Minimum length for a valid symbol

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the trade validator.

        Args:
            app_settings: Application settings with portfolio configuration
        """
        self.app_settings = app_settings
        self.validation_config = app_settings.portfolio_tracker.validation

        self.min_price = self.validation_config.min_price
        self.max_price = self.validation_config.max_price
        self.min_quantity = self.validation_config.min_quantity
        self.max_quantity = self.validation_config.max_quantity
        self.min_trade_value = self.validation_config.min_trade_value
        self.max_trade_value = self.validation_config.max_trade_value

    def validate_trade(self, trade: Trade) -> ValidationResult[Trade]:
        """Validate a single trade.
        
        Args:
            trade: The trade to validate.
            
        Returns:
            ValidationResult containing the trade and any validation issues found.
        """
        issues: list[ValidationIssue] = []

        # Validate price bounds
        if trade.price < self.min_price or trade.price > self.max_price:
            issues.append(
                create_range_violation_issue(
                    "price",
                    trade.price,
                    self.min_price,
                    self.max_price,
                )
            )

        # Validate quantity bounds
        if trade.quantity < self.min_quantity or trade.quantity > self.max_quantity:
            issues.append(
                create_range_violation_issue(
                    "quantity",
                    trade.quantity,
                    self.min_quantity,
                    self.max_quantity,
                )
            )

        # Validate symbol format
        if len(trade.symbol) < self.MIN_SYMBOL_LENGTH:
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    category=ValidationCategory.INVALID_FORMAT,
                    field="symbol",
                    message=f"Invalid symbol format: {trade.symbol}",
                    code="TRADE_INVALID_SYMBOL",
                    context={"symbol": trade.symbol, "trade_id": trade.id},
                )
            )

        # Validate business rules
        trade_value = trade.price * trade.quantity
        if trade_value < self.min_trade_value:
            issues.append(
                create_business_rule_violation(
                    "MIN_TRADE_VALUE",
                    f"Trade value must be at least {self.min_trade_value}",
                    "trade_value",
                    f"Trade value {trade_value} below minimum {self.min_trade_value}",
                    ValidationSeverity.WARNING,
                )
            )
        elif trade_value > self.max_trade_value:
            issues.append(
                create_business_rule_violation(
                    "MAX_TRADE_VALUE",
                    f"Trade value must not exceed {self.max_trade_value}",
                    "trade_value",
                    f"Trade value {trade_value} above maximum {self.max_trade_value}",
                    ValidationSeverity.WARNING,
                )
            )

        return ValidationResult[Trade].from_issues(trade, issues)


class BalanceValidator:
    """Validates balance data and consistency with direct AppSettings access."""

    # Balance validation constants
    BALANCE_DEVIATION_THRESHOLD = 0.1  # 10% deviation threshold

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the balance validator.

        Args:
            app_settings: Application settings with portfolio configuration
        """
        self.app_settings = app_settings
        self.validation_config = app_settings.portfolio_tracker.validation

        self.min_balance_threshold = self.validation_config.min_balance_threshold
        self.allow_negative_balances = self.validation_config.allow_negative_balances

    def validate_balance(self, balance: SpotBalance) -> ValidationResult[SpotBalance]:
        """Validate a single balance.
        
        Args:
            balance: The spot balance to validate.
            
        Returns:
            ValidationResult containing the balance and any validation issues found.
        """
        # Use validation chain for cleaner validation
        chain = ValidationChain(balance)

        # Check negative balances
        if not self.allow_negative_balances:
            chain.check(
                lambda b: b.available_quantity >= 0,
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    category=ValidationCategory.INVALID_VALUE,
                    field="available_quantity",
                    message=f"Negative available balance: {balance.available_quantity}",
                    code="BALANCE_NEGATIVE_AVAILABLE",
                    context={"available": balance.available_quantity, "asset": balance.asset},
                ),
            )

        # Check minimum threshold
        chain.check(
            lambda b: b.total_quantity >= self.min_balance_threshold,
            ValidationIssue(
                severity=ValidationSeverity.INFO,
                category=ValidationCategory.BUSINESS_RULE,
                field="total_quantity",
                message=f"Total balance below threshold: {balance.total_quantity}",
                code="BALANCE_BELOW_THRESHOLD",
                context={
                    "total": balance.total_quantity,
                    "threshold": self.min_balance_threshold,
                    "asset": balance.asset,
                },
            ),
        )

        # Check consistency between available and total
        chain.check(
            lambda b: b.available_quantity <= b.total_quantity,
            ValidationIssue(
                severity=ValidationSeverity.ERROR,
                category=ValidationCategory.CONSISTENCY,
                field=None,
                message="Available balance exceeds total balance",
                code="BALANCE_CONSISTENCY_ERROR",
                context={
                    "available": balance.available_quantity,
                    "total": balance.total_quantity,
                    "asset": balance.asset,
                },
            ),
        )

        return chain.result()


class PositionValidator:
    """Validates position data and risk limits with direct AppSettings access."""

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the position validator.

        Args:
            app_settings: Application settings with portfolio configuration
        """
        self.app_settings = app_settings
        self.validation_config = app_settings.portfolio_tracker.validation

        self.max_position_size = self.validation_config.max_position_size
        self.max_leverage = self.validation_config.max_leverage
        self.max_position_value = self.validation_config.max_position_value

    def validate_position(
        self, position: DerivativePosition
    ) -> ValidationResult[DerivativePosition]:
        """Validate a single position.
        
        Args:
            position: The derivative position to validate.
            
        Returns:
            ValidationResult containing the position and any validation issues found.
        """
        issues: list[ValidationIssue] = []

        # Validate position size
        if abs(position.size) > self.max_position_size:
            issues.append(
                create_business_rule_violation(
                    "MAX_POSITION_SIZE",
                    f"Position size must not exceed {self.max_position_size}",
                    "quantity",
                    f"Position size {abs(position.size)} exceeds maximum {self.max_position_size}",
                )
            )

        # Validate leverage (check in exchange-specific details)
        if position.hl_details and position.hl_details.leverage_value > self.max_leverage:
            issues.append(
                create_business_rule_violation(
                    "MAX_LEVERAGE",
                    f"Leverage must not exceed {self.max_leverage}x",
                    "leverage",
                    f"Leverage {position.hl_details.leverage_value}x exceeds "
                    f"maximum {self.max_leverage}x",
                )
            )
        elif (
            position.bp_details
            and position.bp_details.leverage
            and position.bp_details.leverage > self.max_leverage
        ):
            # BackpackPositionDetails now has leverage field
            issues.append(
                create_business_rule_violation(
                    "MAX_LEVERAGE",
                    f"Leverage must not exceed {self.max_leverage}x",
                    "leverage",
                    f"Leverage {position.bp_details.leverage}x exceeds "
                    f"maximum {self.max_leverage}x",
                )
            )

        # Validate position value
        if position.mark_price and position.size:
            position_value = abs(position.mark_price * position.size)
            if position_value > self.max_position_value:
                issues.append(
                    create_business_rule_violation(
                        "MAX_POSITION_VALUE",
                        f"Position value must not exceed {self.max_position_value}",
                        "position_value",
                        f"Position value {position_value} exceeds "
                        f"maximum {self.max_position_value}",
                    )
                )

        # Check if PnL values are provided and reasonable
        if position.unrealized_pnl is not None and position.realized_pnl is not None:
            # Could add additional PnL validation logic here if needed
            pass

        return ValidationResult[DerivativePosition].from_issues(position, issues)


class PortfolioValidationService:
    """Main portfolio validation service with direct AppSettings access.

    Follows risk module patterns:
    - Direct AppSettings access
    - Protocol-based dependencies
    - No inheritance from base services
    - Configuration from AppSettings
    """

    def __init__(
        self,
        app_settings: AppSettings,
        state_container: StateContainerProtocol[BaseStateModel],
    ) -> None:
        """Initialize portfolio validation service.

        Args:
            app_settings: Application settings with portfolio configuration
            state_container: State container for accessing portfolio data
        """
        self.app_settings = app_settings
        self.portfolio_config = app_settings.portfolio_tracker
        self.validation_config = app_settings.portfolio_tracker.validation
        self.state_container = state_container
        self.logger = get_logger(self.__class__.__name__)

        # Initialize validators with AppSettings
        self.trade_validator = TradeValidator(app_settings)
        self.balance_validator = BalanceValidator(app_settings)
        self.position_validator = PositionValidator(app_settings)

        # Validation statistics
        self.validation_stats: dict[str, dict[str, int]] = defaultdict(
            lambda: {"total": 0, "passed": 0, "failed": 0}
        )
        self.recent_issues: list[ValidationIssue] = []
        self.max_recent_issues = self.validation_config.max_recent_issues

        self.logger.info(
            "portfolio_validation_service_initialized",
            validation_config_summary={
                "max_recent_issues": self.max_recent_issues,
                "min_price": self.validation_config.min_price,
                "max_price": self.validation_config.max_price,
            },
        )

    async def validate_trade(self, trade: Trade) -> ValidationResult[Trade]:
        """Validate a trade.
        
        Args:
            trade: The trade to validate.
            
        Returns:
            ValidationResult containing the trade and any validation issues found.
        """
        result = self.trade_validator.validate_trade(trade)
        self._record_validation("trade", result)
        return result

    async def validate_balance(self, balance: SpotBalance) -> ValidationResult[SpotBalance]:
        """Validate a balance.
        
        Args:
            balance: The spot balance to validate.
            
        Returns:
            ValidationResult containing the balance and any validation issues found.
        """
        result = self.balance_validator.validate_balance(balance)
        self._record_validation("balance", result)
        return result

    async def validate_position(
        self, position: DerivativePosition
    ) -> ValidationResult[DerivativePosition]:
        """Validate a position.
        
        Args:
            position: The derivative position to validate.
            
        Returns:
            ValidationResult containing the position and any validation issues found.
        """
        result = self.position_validator.validate_position(position)
        self._record_validation("position", result)
        return result

    async def validate_batch_trades(self, trades: list[Trade]) -> ValidationResult[list[Trade]]:
        """Validate a batch of trades.
        
        Args:
            trades: List of trades to validate.
            
        Returns:
            ValidationResult containing all trades and accumulated validation issues.
        """
        all_issues: list[ValidationIssue] = []

        for trade in trades:
            trade_result = await self.validate_trade(trade)
            all_issues.extend(trade_result.issues)

        return ValidationResult[list[Trade]].from_issues(trades, all_issues)

    async def validate_portfolio_state(self) -> ValidationResult[PortfolioState]:
        """Validate overall portfolio state using state container.
        
        Returns:
            ValidationResult containing the portfolio state and any validation issues found.
        """
        issues: list[ValidationIssue] = []

        # Build portfolio state with typed models
        balances_dict: dict[str, ExchangeBalances] = {}
        positions_dict: dict[str, ExchangePositions] = {}
        current_time = time.time()

        try:
            # Get all exchange names from config
            exchange_names = list(self.portfolio_config.initial_balances.keys())

            # Validate all balances
            for exchange_name in exchange_names:
                try:
                    # Convert string to ExchangeName enum
                    exchange_enum = ExchangeName(exchange_name)
                    balances = self.state_container.get_balances(exchange_enum)

                    # Create typed exchange balances
                    exchange_balances = ExchangeBalances(
                        exchange_name=exchange_name,
                        balances=balances,
                        last_update=current_time,
                        is_stale=False,
                    )
                    balances_dict[exchange_name] = exchange_balances

                    # Validate each balance
                    for balance in balances.values():
                        balance_result = await self.validate_balance(balance)
                        issues.extend(balance_result.issues)

                except (ValueError, TypeError, KeyError, AttributeError) as e:
                    self.logger.warning(
                        "Failed to get balances", exchange=exchange_name, error=str(e)
                    )

            # Validate all positions
            for exchange_name in exchange_names:
                try:
                    # Convert string to ExchangeName enum
                    exchange_enum = ExchangeName(exchange_name)
                    positions = self.state_container.get_positions(exchange_enum)

                    # Create typed exchange positions
                    exchange_positions = ExchangePositions(
                        exchange_name=exchange_name,
                        positions=positions,
                        last_update=current_time,
                        is_stale=False,
                    )
                    positions_dict[exchange_name] = exchange_positions

                    # Validate each position
                    for position in positions.values():
                        position_result = await self.validate_position(position)
                        issues.extend(position_result.issues)

                except (ValueError, TypeError, KeyError, AttributeError) as e:
                    self.logger.warning(
                        "Failed to get positions", exchange=exchange_name, error=str(e)
                    )

            # Build complete portfolio state
            portfolio_state = PortfolioState(
                balances=balances_dict,
                positions=positions_dict,
                orders={},  # Orders handled separately
                metrics=PortfolioMetrics(last_update_timestamp=current_time),
                version=1,
                last_update=current_time,
                is_consistent=len(issues) == 0,
            )

            # Cross-validation checks
            cross_validation_issues = await self._validate_cross_consistency(portfolio_state)
            issues.extend(cross_validation_issues)

        except Exception as e:
            self.logger.exception("portfolio_state_validation_error")
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    category=ValidationCategory.INVALID_STATE,
                    field=None,
                    message=f"Failed to validate portfolio state: {e!s}",
                    code="PORTFOLIO_VALIDATION_ERROR",
                )
            )

            # Return empty portfolio state on error
            portfolio_state = PortfolioState(
                version=0, last_update=current_time, is_consistent=False
            )

        return ValidationResult[PortfolioState].from_issues(portfolio_state, issues)

    async def _validate_cross_consistency(
        self, portfolio_state: PortfolioState
    ) -> list[ValidationIssue]:
        """Validate cross-component consistency.
        
        Args:
            portfolio_state: The portfolio state to validate for cross-consistency.
            
        Returns:
            List of validation issues found during cross-consistency checks.
        """
        issues: list[ValidationIssue] = []

        # Add cross-validation logic here
        # For example: check that position margins match balance locks

        return issues

    def _record_validation(self, validation_type: str, result: ValidationResult[Any]) -> None:
        """Record validation statistics.
        
        Args:
            validation_type: Type of validation being recorded (e.g., 'trade', 'balance').
            result: The validation result to record.
        """
        self.validation_stats[validation_type]["total"] += 1
        if result.is_valid:
            self.validation_stats[validation_type]["passed"] += 1
        else:
            self.validation_stats[validation_type]["failed"] += 1

        # Store recent issues
        for issue in result.issues:
            self.recent_issues.append(issue)
            if len(self.recent_issues) > self.max_recent_issues:
                self.recent_issues.pop(0)

    def get_validation_stats(self) -> ValidationStatistics:
        """Get validation statistics.
        
        Returns:
            ValidationStatistics containing validation counts and recent issues.
        """
        # Create proper IssueDict entries - filter out None values
        recent_issues_data: list[IssueDict] = []
        for issue in self.recent_issues[-20:]:  # Last 20 issues
            issue_dict: IssueDict = {
                "severity": issue.severity.value,
                "category": issue.category.value,
                "message": issue.message,
            }
            if issue.field is not None:
                issue_dict["field"] = issue.field
            if issue.code is not None:
                issue_dict["code"] = issue.code
            recent_issues_data.append(issue_dict)

        return ValidationStatistics(
            stats=dict(self.validation_stats),
            recent_issues=recent_issues_data,
            timestamp=time.time(),
        )
