"""Portfolio reconciliation service for data integrity verification."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, NamedTuple

from pydantic import BaseModel, Field, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.portfolio.exceptions.service import (
    ReconciliationDiscrepancyTypeError,
    ReconciliationSeverityError,
    ReconciliationValueError,
)
from cyberdelta.core.portfolio.services.base.base_service import (
    BasePortfolioService,
    ServiceConfiguration,
)
from cyberdelta.enums import OrderType


if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition, Order, SpotBalance, Trade

logger = get_logger(__name__)


class ReconciliationDiscrepancyMetadata(BaseModel):
    """Typed metadata for reconciliation discrepancies."""

    reconciliation_timestamp: float | None = None
    data_source: str | None = None
    check_method: str | None = None
    tolerance_used: Decimal | None = None
    related_transactions: list[str] = Field(default_factory=list)
    confidence_score: float | None = None
    auto_fixable: bool = False
    fix_suggestions: list[str] = Field(default_factory=list)

    # Allow additional dynamic fields for discrepancy-specific data
    additional_data: dict[str, str | int | float | bool] = Field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> ReconciliationDiscrepancyMetadata | None:
        """Create metadata from dict, extracting known fields.
        
        Returns:
            ReconciliationDiscrepancyMetadata instance or None if data is empty.
        """
        if not data:
            return None

        # Extract known fields with proper typing
        known_fields: dict[str, Any] = {}
        additional_data: dict[str, str | int | float | bool] = {}

        for key, value in data.items():
            if key in cls.model_fields:
                known_fields[key] = value
            # Store unknown fields in additional_data
            elif isinstance(value, (str, int, float, bool)):
                additional_data[key] = value
            else:
                # Convert other types to string
                additional_data[key] = str(value)

        known_fields["additional_data"] = additional_data
        return cls(**known_fields)


class ReconciliationServiceConfiguration(ServiceConfiguration):
    """Configuration for portfolio reconciliation service with validation."""

    # Reconciliation settings
    tolerance: Decimal = Field(
        default=Decimal("0.00001"), gt=0, description="Tolerance for reconciliation checks"
    )
    check_balance_consistency: bool = Field(
        default=True, description="Enable balance consistency checks"
    )
    check_position_consistency: bool = Field(
        default=True, description="Enable position consistency checks"
    )
    check_order_consistency: bool = Field(
        default=True, description="Enable order consistency checks"
    )
    check_trade_integrity: bool = Field(default=True, description="Enable trade integrity checks")

    # Warning thresholds
    balance_warning_threshold: Decimal = Field(
        default=Decimal("0.01"), gt=0, description="Balance warning threshold"
    )
    position_warning_threshold: Decimal = Field(
        default=Decimal("0.01"), gt=0, description="Position warning threshold"
    )

    # Limits and constraints
    max_discrepancies_per_type: int = Field(
        default=100, gt=0, description="Maximum discrepancies per type"
    )
    allow_negative_balances: bool = Field(default=False, description="Allow negative balances")
    max_balance_warning: Decimal = Field(
        default=Decimal(1000000), gt=0, description="Maximum balance before warning"
    )
    max_position_size: Decimal = Field(
        default=Decimal(1000000), gt=0, description="Maximum position size"
    )
    max_position_value: Decimal = Field(
        default=Decimal(10000000), gt=0, description="Maximum position value"
    )
    max_order_age_seconds: int = Field(
        default=86400, gt=0, description="Maximum order age in seconds"
    )

    @field_validator(
        "tolerance",
        "balance_warning_threshold",
        "position_warning_threshold",
        "max_balance_warning",
        "max_position_size",
        "max_position_value",
        mode="before",
    )
    @classmethod
    def validate_decimal_fields(cls, v: Decimal | str | float) -> Decimal:
        """Validate Decimal fields are positive and finite.
        
        Returns:
            Validated Decimal value.
            
        Raises:
            ReconciliationValueError: If value is not positive or finite.
        """
        decimal_v = Decimal(str(v)) if isinstance(v, (str, float, int)) else v
        # decimal_v is now guaranteed to be Decimal

        if not decimal_v.is_finite() or decimal_v <= 0:
            raise ReconciliationValueError(value_type="decimal", actual_value=str(decimal_v))
        return decimal_v


@dataclass
class ReconciliationDiscrepancy:
    """Represents a discrepancy found during reconciliation."""

    type: str = Field(description="Discrepancy type: balance, position, order, or trade")
    severity: str = Field(description="Severity level: error, warning, or info")
    description: str = Field(min_length=1, description="Discrepancy description")
    expected_value: Any = Field(description="Expected value")
    actual_value: Any = Field(description="Actual value")
    exchange_id: str = Field(min_length=1, description="Exchange identifier")
    symbol: str | None = Field(default=None, description="Trading symbol")
    currency: str | None = Field(default=None, pattern="^[A-Z]{3,4}$", description="Currency code")
    metadata: ReconciliationDiscrepancyMetadata | dict[str, Any] | None = Field(
        default=None, description="Additional metadata"
    )

    @field_validator("metadata", mode="before")
    @classmethod
    def validate_metadata(
        cls, v: dict[str, Any] | ReconciliationDiscrepancyMetadata | None
    ) -> ReconciliationDiscrepancyMetadata | None:
        """Convert dict to ReconciliationDiscrepancyMetadata if needed.
        
        Returns:
            ReconciliationDiscrepancyMetadata instance or None.
        """
        if v is None:
            return None
        if isinstance(v, dict):
            return ReconciliationDiscrepancyMetadata.from_dict(v)
        # v is already ReconciliationDiscrepancyMetadata by type annotation
        return v

    @field_validator("type", mode="before")
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Validate discrepancy type.
        
        Returns:
            Validated discrepancy type string.
            
        Raises:
            ReconciliationDiscrepancyTypeError: If type is not valid.
        """
        valid_types = {"balance", "position", "order", "trade"}
        if v not in valid_types:
            raise ReconciliationDiscrepancyTypeError(invalid_type=v, valid_types=list(valid_types))
        return v

    @field_validator("severity", mode="before")
    @classmethod
    def validate_severity(cls, v: str) -> str:
        """Validate severity level.
        
        Returns:
            Validated severity level string.
            
        Raises:
            ReconciliationSeverityError: If severity level is not valid.
        """
        valid_severities = {"error", "warning", "info"}
        if v not in valid_severities:
            raise ReconciliationSeverityError(
                invalid_severity=v, valid_severities=list(valid_severities)
            )
        return v


class ReconciliationResult(NamedTuple):
    """Result of portfolio reconciliation."""

    is_reconciled: bool
    total_discrepancies: int
    errors: list[ReconciliationDiscrepancy]
    warnings: list[ReconciliationDiscrepancy]
    info: list[ReconciliationDiscrepancy]
    summary: dict[str, Any]


class PortfolioReconciliationService(BasePortfolioService):
    """Service for reconciling portfolio data across exchanges and components.

    This service ensures data integrity by:
    - Verifying balance consistency across exchanges
    - Validating position calculations
    - Checking order status consistency
    - Detecting data corruption or inconsistencies
    """

    # Thresholds for reconciliation checks
    BALANCE_CONCENTRATION_THRESHOLD = 90  # Percentage threshold for balance concentration warning
    INTEGRITY_SCORE_HEALTHY_THRESHOLD = 95  # Minimum integrity score for healthy portfolio
    WARNING_COUNT_THRESHOLD = 10  # Maximum warnings before recommending review

    def __init__(
        self,
        name: str = "PortfolioReconciliationService",
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the reconciliation service.

        Args:
            name: Service name
            config: Configuration dictionary
        """
        # Convert raw config to validated ReconciliationServiceConfiguration
        config_dict = dict(config) if config else {}
        config_dict["name"] = name  # Ensure name is set

        try:
            self.recon_config = ReconciliationServiceConfiguration.model_validate(config_dict)
        except (ValueError, TypeError, AttributeError) as e:
            # Fallback to default config with provided name if validation fails
            logger.warning(
                "reconciliation_config_validation_failed",
                service_name=name,
                error=str(e),
                fallback_to_default=True,
            )
            self.recon_config = ReconciliationServiceConfiguration(name=name)

        # Pass the validated config to parent
        super().__init__(name, self.recon_config.model_dump())

        # Set reconciliation-specific attributes for easy access
        self.tolerance = self.recon_config.tolerance
        self.check_balance_consistency = self.recon_config.check_balance_consistency
        self.check_position_consistency = self.recon_config.check_position_consistency
        self.check_order_consistency = self.recon_config.check_order_consistency
        self.check_trade_integrity = self.recon_config.check_trade_integrity
        self.balance_warning_threshold = self.recon_config.balance_warning_threshold
        self.position_warning_threshold = self.recon_config.position_warning_threshold
        self.max_discrepancies_per_type = self.recon_config.max_discrepancies_per_type

        logger.info(
            "portfolio_reconciliation_service_created",
            service_name=name,
            tolerance=self.tolerance,
            check_balance_consistency=self.check_balance_consistency,
            check_position_consistency=self.check_position_consistency,
        )

    async def _start_internal(self) -> None:
        """Initialize the reconciliation service."""
        logger.info("portfolio_reconciliation_service_initializing")

    async def _stop_internal(self) -> None:
        """Shutdown the reconciliation service."""
        logger.info("portfolio_reconciliation_service_shutting_down")

    async def reconcile_portfolio(
        self,
        balances: dict[str, dict[str, SpotBalance]],
        positions: dict[str, dict[str, DerivativePosition]],
        orders: dict[str, dict[str, Order]],
        trades: list[Trade] | None = None,
    ) -> ReconciliationResult:
        """Perform comprehensive portfolio reconciliation.

        Args:
            balances: Balances by exchange and currency
            positions: Positions by exchange and symbol
            orders: Orders by exchange and order ID
            trades: Optional list of trades for integrity checking

        Returns:
            ReconciliationResult with findings
            
        Raises:
            RuntimeError: If service is not running.
        """
        if not self.is_running:
            raise RuntimeError

        discrepancies: list[ReconciliationDiscrepancy] = []

        # Check balance consistency
        if self.check_balance_consistency:
            balance_discrepancies = await self._reconcile_balances(balances)
            discrepancies.extend(balance_discrepancies)

        # Check position consistency
        if self.check_position_consistency:
            position_discrepancies = await self._reconcile_positions(positions)
            discrepancies.extend(position_discrepancies)

        # Check order consistency
        if self.check_order_consistency:
            order_discrepancies = await self._reconcile_orders(orders)
            discrepancies.extend(order_discrepancies)

        # Check trade integrity
        if self.check_trade_integrity and trades:
            trade_discrepancies = await self._reconcile_trades(trades, positions)
            discrepancies.extend(trade_discrepancies)

        # Cross-exchange reconciliation
        cross_exchange_discrepancies = await self._reconcile_cross_exchange(balances, positions)
        discrepancies.extend(cross_exchange_discrepancies)

        # Categorize discrepancies
        errors = [d for d in discrepancies if d.severity == "error"]
        warnings = [d for d in discrepancies if d.severity == "warning"]
        info = [d for d in discrepancies if d.severity == "info"]

        # Generate summary
        summary = {
            "total_exchanges": len({d.exchange_id for d in discrepancies}),
            "total_symbols": len({d.symbol for d in discrepancies if d.symbol}),
            "total_currencies": len({d.currency for d in discrepancies if d.currency}),
            "discrepancies_by_type": {
                "balance": len([d for d in discrepancies if d.type == "balance"]),
                "position": len([d for d in discrepancies if d.type == "position"]),
                "order": len([d for d in discrepancies if d.type == "order"]),
                "trade": len([d for d in discrepancies if d.type == "trade"]),
            },
            "discrepancies_by_exchange": {},
        }

        discrepancies_by_exchange = {}
        for exchange_id in {d.exchange_id for d in discrepancies}:
            discrepancies_by_exchange[exchange_id] = len([
                d for d in discrepancies if d.exchange_id == exchange_id
            ])
        summary["discrepancies_by_exchange"] = discrepancies_by_exchange

        is_reconciled = len(errors) == 0

        result = ReconciliationResult(
            is_reconciled=is_reconciled,
            total_discrepancies=len(discrepancies),
            errors=errors,
            warnings=warnings,
            info=info,
            summary=summary,
        )

        logger.info(
            "portfolio_reconciliation_completed",
            is_reconciled=is_reconciled,
            total_discrepancies=len(discrepancies),
            errors=len(errors),
            warnings=len(warnings),
            info=len(info),
        )

        return result

    async def _reconcile_balances(
        self, balances: dict[str, dict[str, SpotBalance]]
    ) -> list[ReconciliationDiscrepancy]:
        """Reconcile balance data for consistency.

        Args:
            balances: Balances by exchange and currency

        Returns:
            List of balance discrepancies
        """
        discrepancies: list[ReconciliationDiscrepancy] = []

        for exchange_id, exchange_balances in balances.items():
            for currency, balance in exchange_balances.items():
                try:
                    # Check balance internal consistency
                    total = balance.total_quantity
                    available = balance.available_quantity
                    # SpotBalance model doesn't have locked/reserved fields - use extension details
                    locked = Decimal(0)
                    reserved = Decimal(0)

                    # Extract locked amounts dynamically from bp_details
                    if balance.bp_details:
                        locked += self._extract_locked_amounts(balance.bp_details)

                    expected_total = available + locked + reserved

                    if abs(total - expected_total) > self.tolerance:
                        severity = (
                            "error"
                            if abs(total - expected_total) > self.balance_warning_threshold
                            else "warning"
                        )
                        discrepancies.append(
                            ReconciliationDiscrepancy(
                                type="balance",
                                severity=severity,
                                description="Balance components do not sum to total",
                                expected_value=float(expected_total),
                                actual_value=float(total),
                                exchange_id=exchange_id,
                                currency=currency,
                                metadata={
                                    "available": float(available),
                                    "locked": float(locked),
                                    "reserved": float(reserved),
                                    "difference": float(total - expected_total),
                                },
                            )
                        )

                    # Check for negative balances (if not allowed)
                    if total < 0 and not self.recon_config.allow_negative_balances:
                        discrepancies.append(
                            ReconciliationDiscrepancy(
                                type="balance",
                                severity="error",
                                description="Negative total balance detected",
                                expected_value=0.0,
                                actual_value=float(total),
                                exchange_id=exchange_id,
                                currency=currency,
                            )
                        )

                    if available < 0:
                        discrepancies.append(
                            ReconciliationDiscrepancy(
                                type="balance",
                                severity="error",
                                description="Negative available balance detected",
                                expected_value=0.0,
                                actual_value=float(available),
                                exchange_id=exchange_id,
                                currency=currency,
                            )
                        )

                    # Check for unusually large balances
                    if total > self.recon_config.max_balance_warning:
                        discrepancies.append(
                            ReconciliationDiscrepancy(
                                type="balance",
                                severity="warning",
                                description="Unusually large balance detected",
                                expected_value=float(self.recon_config.max_balance_warning),
                                actual_value=float(total),
                                exchange_id=exchange_id,
                                currency=currency,
                            )
                        )

                except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
                    discrepancies.append(
                        ReconciliationDiscrepancy(
                            type="balance",
                            severity="error",
                            description=f"Error processing balance: {e!s}",
                            expected_value=None,
                            actual_value=None,
                            exchange_id=exchange_id,
                            currency=currency,
                        )
                    )

        return discrepancies[: self.max_discrepancies_per_type]

    async def _reconcile_positions(
        self, positions: dict[str, dict[str, DerivativePosition]]
    ) -> list[ReconciliationDiscrepancy]:
        """Reconcile position data for consistency.

        Args:
            positions: Positions by exchange and symbol

        Returns:
            List of position discrepancies
        """
        discrepancies: list[ReconciliationDiscrepancy] = []

        for exchange_id, exchange_positions in positions.items():
            for symbol, position in exchange_positions.items():
                try:
                    # Check position size consistency
                    size = position.size
                    entry_price = position.entry_price

                    # Check for zero-sized positions with non-zero entry price
                    if size == 0 and entry_price != 0:
                        discrepancies.append(
                            ReconciliationDiscrepancy(
                                type="position",
                                severity="warning",
                                description="Zero-sized position with non-zero entry price",
                                expected_value=0.0,
                                actual_value=float(entry_price) if entry_price else 0.0,
                                exchange_id=exchange_id,
                                symbol=symbol,
                            )
                        )

                    # Check for positions with zero or negative entry price
                    if size != 0 and (entry_price is None or entry_price <= 0):
                        discrepancies.append(
                            ReconciliationDiscrepancy(
                                type="position",
                                severity="error",
                                description="Position with zero or negative entry price",
                                expected_value=1.0,  # Minimum expected price
                                actual_value=float(entry_price) if entry_price else None,
                                exchange_id=exchange_id,
                                symbol=symbol,
                            )
                        )

                    # Check for unusually large positions
                    if abs(size) > self.recon_config.max_position_size:
                        discrepancies.append(
                            ReconciliationDiscrepancy(
                                type="position",
                                severity="warning",
                                description="Unusually large position size",
                                expected_value=float(self.recon_config.max_position_size),
                                actual_value=float(abs(size)),
                                exchange_id=exchange_id,
                                symbol=symbol,
                            )
                        )

                    # Check position value consistency
                    if size != 0 and entry_price is not None and entry_price > 0:
                        position_value = abs(size) * entry_price

                        if position_value > self.recon_config.max_position_value:
                            discrepancies.append(
                                ReconciliationDiscrepancy(
                                    type="position",
                                    severity="warning",
                                    description="Position value exceeds threshold",
                                    expected_value=float(self.recon_config.max_position_value),
                                    actual_value=float(position_value),
                                    exchange_id=exchange_id,
                                    symbol=symbol,
                                    metadata={
                                        "position_size": float(size),
                                        "entry_price": float(entry_price),
                                    },
                                )
                            )

                except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
                    discrepancies.append(
                        ReconciliationDiscrepancy(
                            type="position",
                            severity="error",
                            description=f"Error processing position: {e!s}",
                            expected_value=None,
                            actual_value=None,
                            exchange_id=exchange_id,
                            symbol=symbol,
                        )
                    )

        return discrepancies[: self.max_discrepancies_per_type]

    async def _reconcile_orders(
        self, orders: dict[str, dict[str, Order]]
    ) -> list[ReconciliationDiscrepancy]:
        """Reconcile order data for consistency.

        Args:
            orders: Orders by exchange and order ID

        Returns:
            List of order discrepancies
        """
        discrepancies: list[ReconciliationDiscrepancy] = []

        for exchange_id, exchange_orders in orders.items():
            for order_id, order in exchange_orders.items():
                try:
                    # Check order quantity consistency
                    quantity = order.quantity_requested
                    filled_quantity = order.quantity_filled

                    if filled_quantity > quantity:
                        discrepancies.append(
                            ReconciliationDiscrepancy(
                                type="order",
                                severity="error",
                                description="Filled quantity exceeds order quantity",
                                expected_value=float(quantity),
                                actual_value=float(filled_quantity),
                                exchange_id=exchange_id,
                                symbol=order.symbol,
                                metadata={"order_id": order_id},
                            )
                        )

                    # Check order status consistency
                    status = order.status
                    if (
                        status in {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED}
                        and filled_quantity == 0
                    ) and status == OrderStatus.FILLED:
                        discrepancies.append(
                            ReconciliationDiscrepancy(
                                type="order",
                                severity="error",
                                description="Order marked as filled but has zero filled quantity",
                                expected_value=float(quantity),
                                actual_value=float(filled_quantity),
                                exchange_id=exchange_id,
                                symbol=order.symbol,
                                metadata={"order_id": order_id, "status": status},
                            )
                        )

                    # Check order price consistency
                    price = order.price
                    order_type = order.order_type

                    if order_type in {OrderType.LIMIT, OrderType.STOP_LIMIT} and (
                        price is None or price <= 0
                    ):
                        discrepancies.append(
                            ReconciliationDiscrepancy(
                                type="order",
                                severity="error",
                                description="Limit order without valid price",
                                expected_value=1.0,  # Minimum expected price
                                actual_value=float(price) if price else None,
                                exchange_id=exchange_id,
                                symbol=order.symbol,
                                metadata={"order_id": order_id, "order_type": order_type},
                            )
                        )

                    # Check for stale orders (most orders have created_at)
                    order_created_at = order.created_at
                    if order_created_at:
                        current_time = datetime.now(UTC)
                        order_age = (current_time - order_created_at).total_seconds()

                        if order_age > self.recon_config.max_order_age_seconds and status in {
                            OrderStatus.NEW,
                            OrderStatus.PARTIALLY_FILLED,
                        }:
                            discrepancies.append(
                                ReconciliationDiscrepancy(
                                    type="order",
                                    severity="warning",
                                    description="Stale order detected",
                                    expected_value=float(self.recon_config.max_order_age_seconds),
                                    actual_value=float(order_age),
                                    exchange_id=exchange_id,
                                    symbol=order.symbol,
                                    metadata={"order_id": order_id, "status": status},
                                )
                            )

                except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
                    discrepancies.append(
                        ReconciliationDiscrepancy(
                            type="order",
                            severity="error",
                            description=f"Error processing order: {e!s}",
                            expected_value=None,
                            actual_value=None,
                            exchange_id=exchange_id,
                            symbol=order.symbol,
                            metadata={"order_id": order_id},
                        )
                    )

        return discrepancies[: self.max_discrepancies_per_type]

    async def _reconcile_trades(
        self, trades: list[Trade], positions: dict[str, dict[str, DerivativePosition]]
    ) -> list[ReconciliationDiscrepancy]:
        """Reconcile trade data for integrity.

        Args:
            trades: List of trades
            positions: Positions by exchange and symbol

        Returns:
            List of trade discrepancies
        """
        discrepancies: list[ReconciliationDiscrepancy] = []

        for trade in trades:
            try:
                exchange_id = trade.exchange
                symbol = trade.symbol

                # Check trade data consistency
                price = trade.price
                quantity = trade.quantity

                if price <= 0:
                    discrepancies.append(
                        ReconciliationDiscrepancy(
                            type="trade",
                            severity="error",
                            description="Trade with zero or negative price",
                            expected_value=1.0,
                            actual_value=float(price),
                            exchange_id=exchange_id,
                            symbol=symbol,
                            metadata={"trade_id": trade.id},
                        )
                    )

                if quantity <= 0:
                    discrepancies.append(
                        ReconciliationDiscrepancy(
                            type="trade",
                            severity="error",
                            description="Trade with zero or negative quantity",
                            expected_value=1.0,
                            actual_value=float(quantity),
                            exchange_id=exchange_id,
                            symbol=symbol,
                            metadata={"trade_id": trade.id},
                        )
                    )

                # Check if trade symbol has corresponding position
                if exchange_id in positions and symbol not in positions[exchange_id]:
                    discrepancies.append(
                        ReconciliationDiscrepancy(
                            type="trade",
                            severity="warning",
                            description="Trade for symbol without corresponding position",
                            expected_value=True,
                            actual_value=False,
                            exchange_id=exchange_id,
                            symbol=symbol,
                            metadata={"trade_id": trade.id},
                        )
                    )

                # Check trade timestamp validity (most trades have timestamp)
                trade_timestamp = trade.executed_at
                if trade_timestamp:
                    current_time = time.time()
                    trade_age = current_time - trade_timestamp.timestamp()

                    if trade_age < 0:
                        discrepancies.append(
                            ReconciliationDiscrepancy(
                                type="trade",
                                severity="error",
                                description="Trade timestamp is in the future",
                                expected_value=current_time,
                                actual_value=trade.executed_at.timestamp(),
                                exchange_id=exchange_id,
                                symbol=symbol,
                                metadata={"trade_id": trade.id},
                            )
                        )

            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
                discrepancies.append(
                    ReconciliationDiscrepancy(
                        type="trade",
                        severity="error",
                        description=f"Error processing trade: {e!s}",
                        expected_value=None,
                        actual_value=None,
                        exchange_id=trade.exchange,
                        symbol=trade.symbol,
                        metadata={"trade_id": trade.id},
                    )
                )

        return discrepancies[: self.max_discrepancies_per_type]

    async def _reconcile_cross_exchange(
        self,
        balances: dict[str, dict[str, SpotBalance]],
        positions: dict[str, dict[str, DerivativePosition]],
    ) -> list[ReconciliationDiscrepancy]:
        """Reconcile data consistency across exchanges.

        Args:
            balances: Balances by exchange and currency
            positions: Positions by exchange and symbol

        Returns:
            List of cross-exchange discrepancies
        """
        discrepancies: list[ReconciliationDiscrepancy] = []

        try:
            # Check position consistency across exchanges
            position_discrepancies = self._check_cross_exchange_positions(positions)
            discrepancies.extend(position_discrepancies)

            # Check balance distribution across exchanges
            balance_discrepancies = self._check_cross_exchange_balances(balances)
            discrepancies.extend(balance_discrepancies)

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            discrepancies.append(
                ReconciliationDiscrepancy(
                    type="cross_exchange",
                    severity="error",
                    description=f"Error in cross-exchange reconciliation: {e!s}",
                    expected_value=None,
                    actual_value=None,
                    exchange_id="all",
                )
            )

        return discrepancies[: self.max_discrepancies_per_type]

    def _check_cross_exchange_positions(
        self, positions: dict[str, dict[str, DerivativePosition]]
    ) -> list[ReconciliationDiscrepancy]:
        """Check position consistency across exchanges.
        
        Returns:
            List of cross-exchange position discrepancies.
        """
        discrepancies: list[ReconciliationDiscrepancy] = []

        # Build symbol to exchanges mapping
        symbol_exchanges = self._build_symbol_exchange_map(positions)

        # Look for symbols that appear on multiple exchanges
        for symbol, exchanges in symbol_exchanges.items():
            if len(exchanges) > 1:
                discrepancy = self._create_multi_exchange_position_discrepancy(
                    symbol, exchanges, positions
                )
                discrepancies.append(discrepancy)

        return discrepancies

    def _build_symbol_exchange_map(
        self, positions: dict[str, dict[str, DerivativePosition]]
    ) -> dict[str, list[str]]:
        """Build mapping of symbols to exchanges.
        
        Returns:
            Dictionary mapping symbol names to lists of exchange IDs.
        """
        symbol_exchanges: dict[str, list[str]] = {}
        for exchange_id, exchange_positions in positions.items():
            for symbol in exchange_positions:
                if symbol not in symbol_exchanges:
                    symbol_exchanges[symbol] = []
                symbol_exchanges[symbol].append(exchange_id)
        return symbol_exchanges

    def _create_multi_exchange_position_discrepancy(
        self, symbol: str, exchanges: list[str], positions: dict[str, dict[str, DerivativePosition]]
    ) -> ReconciliationDiscrepancy:
        """Create discrepancy for positions on multiple exchanges.
        
        Returns:
            ReconciliationDiscrepancy indicating multi-exchange position.
        """
        position_sizes = {}
        for exchange_id in exchanges:
            position = positions[exchange_id][symbol]
            position_sizes[exchange_id] = position.size

        return ReconciliationDiscrepancy(
            type="position",
            severity="info",
            description="Symbol traded on multiple exchanges",
            expected_value=1,
            actual_value=len(exchanges),
            exchange_id=",".join(exchanges),
            symbol=symbol,
            metadata={"exchanges": exchanges, "position_sizes": position_sizes},
        )

    def _check_cross_exchange_balances(
        self, balances: dict[str, dict[str, SpotBalance]]
    ) -> list[ReconciliationDiscrepancy]:
        """Check balance distribution across exchanges.
        
        Returns:
            List of cross-exchange balance discrepancies.
        """
        discrepancies: list[ReconciliationDiscrepancy] = []

        # Build currency to exchanges mapping
        currency_exchanges = self._build_currency_exchange_map(balances)

        # Look for currencies that appear on multiple exchanges
        for currency, exchanges in currency_exchanges.items():
            if len(exchanges) > 1:
                currency_discrepancies = self._check_currency_distribution(
                    currency, exchanges, balances
                )
                discrepancies.extend(currency_discrepancies)

        return discrepancies

    def _build_currency_exchange_map(
        self, balances: dict[str, dict[str, SpotBalance]]
    ) -> dict[str, list[str]]:
        """Build mapping of currencies to exchanges.
        
        Returns:
            Dictionary mapping currency codes to lists of exchange IDs.
        """
        currency_exchanges: dict[str, list[str]] = {}
        for exchange_id, exchange_balances in balances.items():
            for currency in exchange_balances:
                if currency not in currency_exchanges:
                    currency_exchanges[currency] = []
                currency_exchanges[currency].append(exchange_id)
        return currency_exchanges

    def _check_currency_distribution(
        self, currency: str, exchanges: list[str], balances: dict[str, dict[str, SpotBalance]]
    ) -> list[ReconciliationDiscrepancy]:
        """Check if currency is well distributed across exchanges.
        
        Returns:
            List of currency distribution discrepancies.
        """
        discrepancies: list[ReconciliationDiscrepancy] = []

        # Calculate balance distribution
        balance_amounts: dict[str, Decimal] = {}
        total_balance = Decimal(0)

        for exchange_id in exchanges:
            balance = balances[exchange_id][currency]
            balance_amounts[exchange_id] = balance.total_quantity
            total_balance += balance.total_quantity

        # Check for unusual balance distribution
        if total_balance > 0:
            for exchange_id, amount in balance_amounts.items():
                percentage = (amount / total_balance) * 100
                if percentage > self.BALANCE_CONCENTRATION_THRESHOLD:
                    discrepancies.append(
                        ReconciliationDiscrepancy(
                            type="balance",
                            severity="info",
                            description="High balance concentration on single exchange",
                            expected_value=50.0,  # Expected more balanced distribution
                            actual_value=float(percentage),
                            exchange_id=exchange_id,
                            currency=currency,
                            metadata={
                                "balance_distribution": {
                                    ex: float(amt) for ex, amt in balance_amounts.items()
                                }
                            },
                        )
                    )

        return discrepancies

    async def validate_portfolio_integrity(
        self,
        balances: dict[str, dict[str, SpotBalance]],
        positions: dict[str, dict[str, DerivativePosition]],
        orders: dict[str, dict[str, Order]],
    ) -> dict[str, Any]:
        """Validate overall portfolio integrity.

        Args:
            balances: Balances by exchange and currency
            positions: Positions by exchange and symbol
            orders: Orders by exchange and order ID

        Returns:
            Dictionary with integrity validation results
        """
        reconciliation_result = await self.reconcile_portfolio(balances, positions, orders)

        # Calculate integrity score
        total_checks = (
            len(balances)
            + len(positions)
            + len(orders)
            + sum(len(ex_balances) for ex_balances in balances.values())
            + sum(len(ex_positions) for ex_positions in positions.values())
            + sum(len(ex_orders) for ex_orders in orders.values())
        )

        integrity_score = max(
            0, 100 - (reconciliation_result.total_discrepancies / max(total_checks, 1)) * 100
        )

        return {
            "integrity_score": integrity_score,
            "is_healthy": reconciliation_result.is_reconciled
            and integrity_score > self.INTEGRITY_SCORE_HEALTHY_THRESHOLD,
            "reconciliation_result": reconciliation_result,
            "total_entities_checked": total_checks,
            "recommendations": self._generate_recommendations(reconciliation_result),
        }

    def _generate_recommendations(self, result: ReconciliationResult) -> list[str]:
        """Generate recommendations based on reconciliation results.

        Args:
            result: Reconciliation result

        Returns:
            List of recommendations
        """
        recommendations: list[str] = []

        if result.errors:
            recommendations.append(
                "Immediate attention required: Fix critical data integrity errors"
            )

        if len(result.warnings) > self.WARNING_COUNT_THRESHOLD:
            recommendations.append("Review and address multiple data quality warnings")

        if result.summary["discrepancies_by_type"]["balance"] > 0:
            recommendations.append("Review balance calculation logic and data sources")

        if result.summary["discrepancies_by_type"]["position"] > 0:
            recommendations.append("Verify position size calculations and entry prices")

        if result.summary["discrepancies_by_type"]["order"] > 0:
            recommendations.append("Clean up stale orders and verify order status updates")

        if result.summary["total_exchanges"] > 1:
            recommendations.append("Consider implementing cross-exchange validation rules")

        return recommendations

    def _extract_locked_amounts(self, balance_details: BaseModel) -> Decimal:
        """Extract locked amounts dynamically from balance details.

        This method decouples the reconciliation service from exchange-specific
        implementations by dynamically extracting locked amount fields.

        Args:
            balance_details: Exchange-specific balance details model

        Returns:
            Total locked amount extracted from the balance details
        """
        locked_amount = Decimal(0)

        # Define field names that represent locked amounts
        locked_field_names = [
            "open_order_quantity",
            "lend_quantity",
            "locked_quantity",
            "frozen_quantity",
            "reserved_quantity",
        ]

        # Extract values dynamically
        for field_name in locked_field_names:
            try:
                value = getattr(balance_details, field_name, None)
                if value is not None and isinstance(value, (Decimal, int, float)):
                    locked_amount += Decimal(str(value))
            except (AttributeError, TypeError, ValueError):
                # Skip fields that don't exist or can't be converted
                continue

        return locked_amount
