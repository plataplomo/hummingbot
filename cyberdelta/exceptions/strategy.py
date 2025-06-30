"""Strategy-related exceptions for CyberDelta.

These exceptions handle trading strategy errors including arbitrage failures,
risk management violations, and delta-neutral portfolio issues.
"""

from typing import Any

from cyberdelta.apis.common import APIError, APIErrorCode


class StrategyError(APIError):
    """Base class for trading strategy-related errors."""

    def __init__(
        self,
        message: str,
        *,
        strategy_name: str | None = None,
        code: int | str | None = None,
        http_status: int | None = None,
        exchange_code: str | int | None = None,
        exchange_message: str | None = None,
        retry_after: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize strategy error.

        Args:
            message: Human-readable error description
            strategy_name: Name of the strategy
            code: Error code
            http_status: HTTP status code
            exchange_code: Exchange-specific error code
            exchange_message: Exchange-specific error message
            retry_after: Seconds to wait before retry
            metadata: Additional error context
            original_exception: The underlying exception
        """
        self.strategy_name = strategy_name

        # Add strategy name to metadata
        if metadata is None:
            metadata = {}
        if strategy_name:
            metadata["strategy_name"] = strategy_name

        super().__init__(
            message=message,
            code=code or APIErrorCode.UNKNOWN.value,
            http_status=http_status,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
            retry_after=retry_after,
            metadata=metadata,
            original_exception=original_exception,
        )


class ArbitrageError(StrategyError):
    """Base class for arbitrage strategy errors."""

    def __init__(
        self,
        message: str,
        *,
        opportunity_id: str | None = None,
        symbol: str | None = None,
        expected_profit: float | str | None = None,
        code: int | str | None = None,
        http_status: int | None = None,
        exchange_code: str | int | None = None,
        exchange_message: str | None = None,
        retry_after: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize arbitrage error.

        Args:
            message: Human-readable error description
            opportunity_id: Unique identifier for the arbitrage opportunity
            symbol: Trading symbol
            expected_profit: Expected profit from the arbitrage
            code: Error code
            http_status: HTTP status code
            exchange_code: Exchange-specific error code
            exchange_message: Exchange-specific error message
            retry_after: Seconds to wait before retry
            metadata: Additional error context
            original_exception: The underlying exception
        """
        self.opportunity_id = opportunity_id
        self.symbol = symbol
        self.expected_profit = expected_profit

        # Add arbitrage context to metadata
        if metadata is None:
            metadata = {}
        metadata.update({
            "opportunity_id": opportunity_id,
            "symbol": symbol,
            "expected_profit": expected_profit,
        })

        super().__init__(
            message=message,
            code=code or APIErrorCode.UNKNOWN.value,
            http_status=http_status,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
            retry_after=retry_after,
            metadata=metadata,
            original_exception=original_exception,
        )


class FundingRateArbitrageError(ArbitrageError):
    """Raised when funding rate arbitrage opportunities fail."""

    def __init__(
        self,
        symbol: str,
        funding_rate: float | str,
        spot_price: float | str | None = None,
        perp_price: float | str | None = None,
        reason: str | None = None,
    ) -> None:
        """Initialize funding rate arbitrage error.

        Args:
            symbol: Trading symbol
            funding_rate: Current funding rate
            spot_price: Spot market price
            perp_price: Perpetual contract price
            reason: Reason for failure
        """
        self.funding_rate = funding_rate
        self.spot_price = spot_price
        self.perp_price = perp_price
        self.reason = reason

        message = f"Funding rate arbitrage failed for {symbol}"
        if reason:
            message = f"{message}: {reason}"

        super().__init__(
            message=message,
            symbol=symbol,
            code=APIErrorCode.UNKNOWN.value,  # No specific arbitrage error code
        )


class DeltaNeutralError(StrategyError):
    """Raised when delta-neutral strategy violations occur."""

    def __init__(
        self,
        current_delta: float | str,
        target_delta: float | str = 0.0,
        tolerance: float | str | None = None,
        portfolio_value: float | str | None = None,
        reason: str | None = None,
    ) -> None:
        """Initialize delta-neutral error.

        Args:
            current_delta: Current portfolio delta
            target_delta: Target delta (usually 0)
            tolerance: Acceptable tolerance
            portfolio_value: Total portfolio value
            reason: Reason for violation
        """
        self.current_delta = current_delta
        self.target_delta = target_delta
        self.tolerance = tolerance
        self.portfolio_value = portfolio_value
        self.reason = reason

        message = f"Delta-neutral violation: current delta {current_delta}, target {target_delta}"
        if tolerance:
            message = f"{message} (tolerance: {tolerance})"
        if reason:
            message = f"{message}: {reason}"

        super().__init__(
            message=message,
            strategy_name="delta_neutral",
            code=APIErrorCode.UNKNOWN.value,  # No specific delta neutral violation code
            metadata={
                "current_delta": current_delta,
                "target_delta": target_delta,
                "tolerance": tolerance,
                "portfolio_value": portfolio_value,
                "reason": reason,
            },
        )


class RiskLimitError(StrategyError):
    """Raised when risk limits are exceeded."""

    def __init__(
        self,
        limit_type: str,
        current_value: float | str,
        limit_value: float | str,
        symbol: str | None = None,
        strategy_name: str | None = None,
    ) -> None:
        """Initialize risk limit error.

        Args:
            limit_type: Type of limit (e.g., 'position_size', 'exposure', 'var')
            current_value: Current value
            limit_value: Limit threshold
            symbol: Optional trading symbol
            strategy_name: Optional strategy name
        """
        self.limit_type = limit_type
        self.current_value = current_value
        self.limit_value = limit_value
        self.symbol = symbol

        message = f"Risk limit exceeded: {limit_type} {current_value} > {limit_value}"
        if symbol:
            message = f"{message} for {symbol}"

        super().__init__(
            message=message,
            strategy_name=strategy_name,
            code=APIErrorCode.UNKNOWN.value,  # No specific risk limit exceeded code
            metadata={
                "limit_type": limit_type,
                "current_value": current_value,
                "limit_value": limit_value,
                "symbol": symbol,
            },
        )


class PositionSyncError(StrategyError):
    """Raised when position synchronization fails across exchanges."""

    def __init__(
        self,
        symbol: str,
        exchanges: list[str],
        positions: dict[str, Any],
        expected_total: float | str | None = None,
        actual_total: float | str | None = None,
    ) -> None:
        """Initialize position sync error.

        Args:
            symbol: Trading symbol
            exchanges: List of exchanges involved
            positions: Dictionary of positions by exchange
            expected_total: Expected total position
            actual_total: Actual total position
        """
        self.symbol = symbol
        self.exchanges = exchanges
        self.positions = positions
        self.expected_total = expected_total
        self.actual_total = actual_total

        message = f"Position sync error for {symbol} across {', '.join(exchanges)}"
        if expected_total is not None and actual_total is not None:
            message = f"{message}: expected {expected_total}, got {actual_total}"

        super().__init__(
            message=message,
            code=APIErrorCode.UNKNOWN.value,  # No specific position sync error code
            metadata={
                "symbol": symbol,
                "exchanges": exchanges,
                "positions": positions,
                "expected_total": expected_total,
                "actual_total": actual_total,
            },
        )


class RebalanceError(StrategyError):
    """Raised when portfolio rebalancing fails."""

    def __init__(
        self,
        target_weights: dict[str, float],
        current_weights: dict[str, float],
        failed_adjustments: list[str],
        reason: str | None = None,
    ) -> None:
        """Initialize rebalance error.

        Args:
            target_weights: Target portfolio weights
            current_weights: Current portfolio weights
            failed_adjustments: List of adjustments that failed
            reason: Reason for failure
        """
        self.target_weights = target_weights
        self.current_weights = current_weights
        self.failed_adjustments = failed_adjustments
        self.reason = reason

        message = f"Portfolio rebalance failed for {', '.join(failed_adjustments)}"
        if reason:
            message = f"{message}: {reason}"

        super().__init__(
            message=message,
            code=APIErrorCode.UNKNOWN.value,  # No specific rebalance failed code
            metadata={
                "target_weights": target_weights,
                "current_weights": current_weights,
                "failed_adjustments": failed_adjustments,
                "reason": reason,
            },
        )
