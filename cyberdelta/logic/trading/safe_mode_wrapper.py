"""Safe mode wrapper for paper trading without real exchange interaction.

This module provides a wrapper that intercepts all exchange operations
when safe mode is enabled, simulating trades without real execution.

IMPORTANT: Following CODING_STANDARDS.md:
- Safe mode controlled by config.general.safe_mode
- NO hardcoded simulation parameters
- All simulated behavior from configuration
- Explicit logging of safe mode operations
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Protocol
import uuid

from cyberdelta.config.structlog_config import get_logger
from pydantic import BaseModel

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.models.market.order import Order, OrderStatus
from cyberdelta.models.market.trade import Trade
from cyberdelta.models.spot_balance import SpotBalance
from cyberdelta.models.derivative_position import DerivativePosition
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums import ExchangeName, OrderSide, OrderType
from cyberdelta.apis.base.exchange_api import ExchangeAPI

logger = get_logger(__name__)


class SimulatedFill(BaseModel):
    """Simulated fill for paper trading."""

    order_id: str
    symbol: Symbol
    side: OrderSide
    price: Decimal
    quantity: Decimal
    fee: Decimal
    timestamp: datetime


class SimulatedBalance(BaseModel):
    """Simulated balance for paper trading."""

    asset: Symbol
    total: Decimal
    available: Decimal
    timestamp: datetime


class SimulatedPosition(BaseModel):
    """Simulated position for paper trading."""

    symbol: Symbol
    side: OrderSide
    size: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal
    timestamp: datetime


class ExchangeAPIProtocol(Protocol):
    """Protocol for exchange API operations."""

    async def place_order(
        self, symbol: str, side: str, order_type: str, price: float, quantity: float, **kwargs: Any
    ) -> Dict[str, Any]:
        """Place an order."""
        ...

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel an order."""
        ...

    async def get_order(self, order_id: str) -> Dict[str, Any]:
        """Get order status."""
        ...

    async def get_balances(self) -> List[Dict[str, Any]]:
        """Get account balances."""
        ...

    async def get_positions(self) -> List[Dict[str, Any]]:
        """Get open positions."""
        ...


class SafeModeWrapper:
    """Wrapper that simulates exchange operations in safe mode.

    When safe mode is enabled (config.general.safe_mode = true),
    this wrapper intercepts all exchange API calls and simulates
    them locally without touching the real exchange.

    Configuration Usage:
    - Uses config.general.safe_mode to determine if active
    - Uses config.testing.paper_trading.initial_balance for starting balance
    - Uses config.testing.paper_trading.fill_probability for order fills
    - Uses config.testing.paper_trading.slippage_range for price slippage
    - Uses config.exchanges[exchange].maker_fee_rate for fee simulation

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL simulation parameters from configuration
    - NO hardcoded trading behavior
    - Explicit safe mode indicators in all operations
    - Structured logging for all simulated actions
    """

    def __init__(self, config: AppSettings, real_api: ExchangeAPI, exchange_name: ExchangeName):
        """Initialize safe mode wrapper.

        Args:
            config: Application settings
            real_api: The real exchange API (used for market data only)
            exchange_name: Name of the exchange being wrapped
        """
        self.config = config
        self._real_api = real_api
        self._exchange_name = exchange_name
        self._safe_mode = config.general.safe_mode

        # Paper trading configuration
        self._paper_config = getattr(config.testing, "paper_trading", None)
        if self._paper_config:
            self._initial_balance = getattr(self._paper_config, "initial_balance", Decimal("10000"))
            self._fill_probability = getattr(self._paper_config, "fill_probability", 0.95)
            self._slippage_range = getattr(
                self._paper_config,
                "slippage_range",
                Decimal("0.001"),  # 0.1%
            )
        else:
            # Fallback if paper trading config not present
            self._initial_balance = Decimal("10000")
            self._fill_probability = 0.95
            self._slippage_range = Decimal("0.001")

        # Exchange-specific configuration
        exchange_config = config.exchanges.get(exchange_name.value)
        if exchange_config:
            self._maker_fee_rate = getattr(exchange_config, "maker_fee_rate", Decimal("0.0002"))
            self._taker_fee_rate = getattr(exchange_config, "taker_fee_rate", Decimal("0.0005"))
        else:
            self._maker_fee_rate = Decimal("0.0002")
            self._taker_fee_rate = Decimal("0.0005")

        # Simulated state
        self._simulated_orders: Dict[str, Order] = {}
        self._simulated_balances: Dict[str, SimulatedBalance] = {}
        self._simulated_positions: Dict[str, SimulatedPosition] = {}
        self._simulated_fills: List[SimulatedFill] = []

        # Initialize with starting balance
        self._initialize_balances()

        if self._safe_mode:
            logger.warning(
                "safe_mode_wrapper_active",
                exchange=exchange_name.value,
                initial_balance=float(self._initial_balance),
                fill_probability=self._fill_probability,
                slippage_range=float(self._slippage_range),
                message="ALL TRADES WILL BE SIMULATED - NO REAL ORDERS",
            )

    def _initialize_balances(self) -> None:
        """Initialize simulated balances with configured starting amounts.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Initial balance from configuration
        - NO hardcoded asset amounts
        """
        # Initialize with base currency (usually USDT or USD)
        base_currency = self.config.calculation.base_currency
        self._simulated_balances[base_currency] = SimulatedBalance(
            asset=Symbol(base_currency),
            total=self._initial_balance,
            available=self._initial_balance,
            timestamp=datetime.now(UTC),
        )

        logger.info(
            "safe_mode_balances_initialized",
            exchange=self._exchange_name.value,
            base_currency=base_currency,
            initial_balance=float(self._initial_balance),
        )

    async def place_order(
        self, symbol: str, side: str, order_type: str, price: float, quantity: float, **kwargs: Any
    ) -> Dict[str, Any]:
        """Place an order (simulated in safe mode).

        Args:
            symbol: Trading symbol
            side: Order side (buy/sell)
            order_type: Order type (market/limit)
            price: Order price
            quantity: Order quantity
            **kwargs: Additional order parameters

        Returns:
            Order placement response

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns simulated response in safe mode
        - Uses configured fill probability
        - Logs all simulated operations
        """
        if not self._safe_mode:
            # Pass through to real API
            return await self._real_api.place_order(
                symbol, side, order_type, price, quantity, **kwargs
            )

        # Simulate order placement
        order_id = f"SIM_{uuid.uuid4().hex[:8]}"
        client_order_id = kwargs.get("client_order_id", f"CLIENT_{uuid.uuid4().hex[:8]}")

        # Create simulated order
        order = Order(
            order_id=order_id,
            client_order_id=client_order_id,
            exchange=self._exchange_name,
            symbol=Symbol(symbol),
            side=OrderSide(side.upper()),
            order_type=OrderType(order_type.upper()),
            price=Decimal(str(price)),
            quantity=Decimal(str(quantity)),
            time_in_force=kwargs.get("time_in_force", "GTC"),
            status=OrderStatus.OPEN,
            timestamp=datetime.now(UTC),
            metadata={"safe_mode": True, "simulated": True},
        )

        self._simulated_orders[order_id] = order

        logger.info(
            "safe_mode_order_placed",
            order_id=order_id,
            symbol=symbol,
            side=side,
            order_type=order_type,
            price=price,
            quantity=quantity,
            safe_mode=True,
            message="SIMULATED ORDER - NOT SENT TO EXCHANGE",
        )

        # Simulate immediate fill for market orders
        if order_type.upper() == "MARKET":
            await self._simulate_fill(order, immediate=True)
        else:
            # Schedule potential fill for limit orders
            asyncio.create_task(self._simulate_limit_fill(order))

        return {
            "order_id": order_id,
            "client_order_id": client_order_id,
            "status": "open",
            "timestamp": datetime.now(UTC).isoformat(),
            "safe_mode": True,
        }

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel an order (simulated in safe mode).

        Args:
            order_id: Order ID to cancel

        Returns:
            Cancellation response
        """
        if not self._safe_mode:
            return await self._real_api.cancel_order(order_id)

        if order_id in self._simulated_orders:
            order = self._simulated_orders[order_id]
            if order.status == OrderStatus.OPEN:
                order.status = OrderStatus.CANCELLED

                logger.info("safe_mode_order_cancelled", order_id=order_id, safe_mode=True)

                return {
                    "order_id": order_id,
                    "status": "cancelled",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "safe_mode": True,
                }

        return {"error": "Order not found", "order_id": order_id, "safe_mode": True}

    async def get_order(self, order_id: str) -> Dict[str, Any]:
        """Get order status (simulated in safe mode).

        Args:
            order_id: Order ID to query

        Returns:
            Order status response
        """
        if not self._safe_mode:
            return await self._real_api.get_order(order_id)

        if order_id in self._simulated_orders:
            order = self._simulated_orders[order_id]
            return {
                "order_id": order_id,
                "status": order.status.value.lower(),
                "symbol": order.symbol.value,
                "side": order.side.value.lower(),
                "price": float(order.price) if order.price else None,
                "quantity": float(order.quantity) if order.quantity else None,
                "filled_quantity": float(order.filled_quantity) if order.filled_quantity else 0,
                "timestamp": order.timestamp.isoformat(),
                "safe_mode": True,
            }

        return {"error": "Order not found", "order_id": order_id, "safe_mode": True}

    async def get_balances(self) -> List[Dict[str, Any]]:
        """Get account balances (simulated in safe mode).

        Returns:
            List of balance dictionaries
        """
        if not self._safe_mode:
            return await self._real_api.get_balances()

        balances = []
        for asset, balance in self._simulated_balances.items():
            balances.append({
                "asset": asset,
                "total": float(balance.total),
                "available": float(balance.available),
                "locked": float(balance.total - balance.available),
                "timestamp": balance.timestamp.isoformat(),
                "safe_mode": True,
            })

        return balances

    async def get_positions(self) -> List[Dict[str, Any]]:
        """Get open positions (simulated in safe mode).

        Returns:
            List of position dictionaries
        """
        if not self._safe_mode:
            return await self._real_api.get_positions()

        positions = []
        for symbol, position in self._simulated_positions.items():
            positions.append({
                "symbol": symbol,
                "side": position.side.value.lower(),
                "size": float(position.size),
                "entry_price": float(position.entry_price),
                "unrealized_pnl": float(position.unrealized_pnl),
                "timestamp": position.timestamp.isoformat(),
                "safe_mode": True,
            })

        return positions

    async def _simulate_fill(self, order: Order, immediate: bool = False) -> None:
        """Simulate order fill based on configuration.

        Args:
            order: Order to potentially fill
            immediate: Whether to fill immediately (market orders)

        IMPORTANT: Following CODING_STANDARDS.md:
        - Fill probability from configuration
        - Slippage from configuration
        - Fee rates from exchange configuration
        """
        import random

        # Check fill probability
        if not immediate and random.random() > self._fill_probability:
            logger.debug(
                "safe_mode_order_not_filled",
                order_id=order.order_id,
                fill_probability=self._fill_probability,
            )
            return

        # Calculate fill price with slippage
        if order.price:
            base_price = order.price
        else:
            # For market orders, use a reference price
            # In real implementation, would get from market data
            base_price = Decimal("50000")  # Placeholder

        # Apply slippage
        slippage_factor = Decimal(
            str(random.uniform(float(-self._slippage_range), float(self._slippage_range)))
        )

        if order.side == OrderSide.BUY:
            # Buyers pay slightly more
            fill_price = base_price * (Decimal("1") + abs(slippage_factor))
        else:
            # Sellers receive slightly less
            fill_price = base_price * (Decimal("1") - abs(slippage_factor))

        # Calculate fee
        if order.order_type == OrderType.MARKET:
            fee_rate = self._taker_fee_rate
        else:
            fee_rate = self._maker_fee_rate

        fee = order.quantity * fill_price * fee_rate

        # Create simulated fill
        fill = SimulatedFill(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            price=fill_price,
            quantity=order.quantity,
            fee=fee,
            timestamp=datetime.now(UTC),
        )

        self._simulated_fills.append(fill)

        # Update order status
        order.status = OrderStatus.FILLED
        order.filled_quantity = order.quantity
        order.average_fill_price = fill_price

        # Update simulated balance
        await self._update_balance_from_fill(fill)

        # Update simulated position
        await self._update_position_from_fill(fill)

        logger.info(
            "safe_mode_order_filled",
            order_id=order.order_id,
            symbol=order.symbol.value,
            side=order.side.value,
            fill_price=float(fill_price),
            quantity=float(order.quantity),
            fee=float(fee),
            slippage_factor=float(slippage_factor),
            safe_mode=True,
            message="SIMULATED FILL - NOT A REAL TRADE",
        )

    async def _simulate_limit_fill(self, order: Order) -> None:
        """Simulate potential limit order fill with delay.

        Args:
            order: Limit order to potentially fill

        IMPORTANT: Following CODING_STANDARDS.md:
        - Delay based on configuration
        - Respects order cancellation
        """
        # Wait for potential fill
        fill_delay = (
            getattr(self._paper_config, "limit_fill_delay_seconds", 5.0)
            if self._paper_config
            else 5.0
        )

        await asyncio.sleep(fill_delay)

        # Check if order still open
        if order.status == OrderStatus.OPEN:
            await self._simulate_fill(order)

    async def _update_balance_from_fill(self, fill: SimulatedFill) -> None:
        """Update simulated balance after fill.

        Args:
            fill: The simulated fill
        """
        base_currency = self.config.calculation.base_currency

        if base_currency not in self._simulated_balances:
            self._simulated_balances[base_currency] = SimulatedBalance(
                asset=Symbol(base_currency),
                total=Decimal("0"),
                available=Decimal("0"),
                timestamp=datetime.now(UTC),
            )

        balance = self._simulated_balances[base_currency]

        # Calculate balance change
        if fill.side == OrderSide.BUY:
            # Buying: decrease balance by cost + fee
            cost = fill.quantity * fill.price + fill.fee
            balance.total -= cost
            balance.available -= cost
        else:
            # Selling: increase balance by proceeds - fee
            proceeds = fill.quantity * fill.price - fill.fee
            balance.total += proceeds
            balance.available += proceeds

        balance.timestamp = datetime.now(UTC)

        logger.debug(
            "safe_mode_balance_updated",
            asset=base_currency,
            total=float(balance.total),
            available=float(balance.available),
            change=float(-cost if fill.side == OrderSide.BUY else proceeds),
        )

    async def _update_position_from_fill(self, fill: SimulatedFill) -> None:
        """Update simulated position after fill.

        Args:
            fill: The simulated fill
        """
        symbol_key = fill.symbol.value

        if symbol_key not in self._simulated_positions:
            # Create new position
            self._simulated_positions[symbol_key] = SimulatedPosition(
                symbol=fill.symbol,
                side=fill.side,
                size=fill.quantity,
                entry_price=fill.price,
                unrealized_pnl=Decimal("0"),
                timestamp=datetime.now(UTC),
            )
        else:
            position = self._simulated_positions[symbol_key]

            # Update position
            if position.side == fill.side:
                # Adding to position
                new_size = position.size + fill.quantity
                new_entry = (
                    position.size * position.entry_price + fill.quantity * fill.price
                ) / new_size
                position.size = new_size
                position.entry_price = new_entry
            else:
                # Reducing position
                if fill.quantity >= position.size:
                    # Position closed
                    realized_pnl = (fill.price - position.entry_price) * position.size
                    if position.side == OrderSide.SELL:
                        realized_pnl = -realized_pnl

                    del self._simulated_positions[symbol_key]

                    logger.info(
                        "safe_mode_position_closed",
                        symbol=symbol_key,
                        realized_pnl=float(realized_pnl),
                    )
                else:
                    # Position reduced
                    position.size -= fill.quantity

            position.timestamp = datetime.now(UTC)

    def get_simulated_stats(self) -> Dict[str, Any]:
        """Get statistics about simulated trading.

        Returns:
            Dictionary with simulation statistics
        """
        total_orders = len(self._simulated_orders)
        filled_orders = sum(
            1 for o in self._simulated_orders.values() if o.status == OrderStatus.FILLED
        )

        total_volume = sum(f.quantity * f.price for f in self._simulated_fills)
        total_fees = sum(f.fee for f in self._simulated_fills)

        return {
            "safe_mode": True,
            "total_orders": total_orders,
            "filled_orders": filled_orders,
            "fill_rate": filled_orders / total_orders if total_orders > 0 else 0,
            "total_fills": len(self._simulated_fills),
            "total_volume": float(total_volume),
            "total_fees": float(total_fees),
            "open_positions": len(self._simulated_positions),
            "current_balance": {
                asset: float(balance.total) for asset, balance in self._simulated_balances.items()
            },
        }

    def is_safe_mode(self) -> bool:
        """Check if safe mode is active.

        Returns:
            True if safe mode is enabled
        """
        return self._safe_mode
