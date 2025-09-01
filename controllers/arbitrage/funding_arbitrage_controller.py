"""
Funding Arbitrage Controller - Portfolio-level management for multi-token funding arbitrage
Manages capital allocation, opportunity ranking, and global risk across multiple funding arbitrage positions
"""
import time
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from pydantic import Field, field_validator

from hummingbot.core.data_type.common import OrderType
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.funding_arbitrage_executor import FundingArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import TripleBarrierConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction


class OpportunityTier(Enum):
    """Opportunity quality tiers"""
    PREMIUM = 1   # >2% spread - highest priority
    STANDARD = 2  # 0.5-2% spread - normal priority
    MARGINAL = 3  # 0.2-0.5% spread - lowest priority


@dataclass
class FundingOpportunity:
    """Represents a funding arbitrage opportunity"""
    token: str
    long_exchange: str
    short_exchange: str
    long_funding_rate: Decimal
    short_funding_rate: Decimal
    spread: Decimal
    expected_profit: Decimal
    required_capital: Decimal
    discovered_at: float = 0  # Timestamp when opportunity was first discovered
    last_updated: float = 0   # Last time this opportunity was refreshed
    tier: OpportunityTier | None = None  # Quality tier

    @property
    def opportunity_id(self) -> str:
        return f"{self.token}_{self.long_exchange}_{self.short_exchange}"

    @property
    def age_seconds(self) -> float:
        """How long this opportunity has existed"""
        return time.time() - self.discovered_at if self.discovered_at else 0

    @property
    def staleness_seconds(self) -> float:
        """How long since this opportunity was last updated"""
        return time.time() - self.last_updated if self.last_updated else 0

    @property
    def priority_score(self) -> Decimal:
        """Calculate priority score for opportunity ranking"""
        # Base score is the spread
        score = self.spread

        # Premium tier gets 3x multiplier, standard 1.5x
        if self.tier == OpportunityTier.PREMIUM:
            score *= Decimal(3)
        elif self.tier == OpportunityTier.STANDARD:
            score *= Decimal("1.5")

        # Decay score based on age (reduce by 10% per hour)
        age_hours = Decimal(str(self.age_seconds / 3600))
        decay_factor = max(Decimal("0.5"), Decimal(1) - (age_hours * Decimal("0.1")))
        score *= decay_factor

        return score


# Order type string-to-enum mapper (only string values supported)
ORDER_TYPE_MAP = {
    "MARKET": OrderType.MARKET,
    "LIMIT": OrderType.LIMIT,
    "LIMIT_MAKER": OrderType.LIMIT_MAKER,
    "AMM_SWAP": OrderType.AMM_SWAP,
}


class FundingArbitrageControllerConfig(ControllerConfigBase):
    """Configuration for the funding arbitrage controller"""
    controller_name: str = "funding_arbitrage_controller"
    controller_type: str = "arbitrage"

    # Exchanges and tokens
    connectors: list[str] = Field(
        default=["backpack_perpetual", "binance_perpetual"],
        json_schema_extra={
            "prompt": "Enter connectors (comma-separated): ",
            "prompt_on_new": True,
        },
    )
    tokens: list[str] = Field(
        default=["BTC", "ETH", "SOL"],
        json_schema_extra={
            "prompt": "Enter tokens to trade (comma-separated): ",
            "prompt_on_new": True,
        },
    )

    # Position sizing and capital management
    position_size_quote: Decimal = Field(
        default=Decimal(50),
        json_schema_extra={
            "prompt": "Position size per trade in quote currency: ",
            "prompt_on_new": True,
        },
    )
    max_total_exposure: Decimal = Field(
        default=Decimal(500),
        json_schema_extra={
            "prompt": "Maximum total portfolio exposure: ",
            "prompt_on_new": True,
        },
    )
    max_positions: int = Field(
        default=5,
        json_schema_extra={
            "prompt": "Maximum number of concurrent positions: ",
            "prompt_on_new": True,
        },
    )

    # Entry conditions with tier thresholds
    min_funding_rate_profitability: Decimal = Field(
        default=Decimal("0.002"),
        json_schema_extra={
            "prompt": "Minimum funding rate spread to enter (e.g. 0.002 for 0.2%): ",
            "prompt_on_new": True,
        },
    )
    premium_tier_threshold: Decimal = Field(
        default=Decimal("0.02"),
        json_schema_extra={
            "prompt": "Minimum spread for premium tier (e.g. 0.02 for 2%): ",
            "prompt_on_new": True,
        },
    )
    standard_tier_threshold: Decimal = Field(
        default=Decimal("0.005"),
        json_schema_extra={
            "prompt": "Minimum spread for standard tier (e.g. 0.005 for 0.5%): ",
            "prompt_on_new": True,
        },
    )
    trade_profitability_condition_to_enter: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": "Check immediate trade profitability before entering? ",
            "prompt_on_new": True,
        },
    )

    # Opportunity management
    max_opportunity_age_hours: float = Field(
        default=1.0,
        json_schema_extra={
            "prompt": "Maximum age for an opportunity in hours (e.g. 1.0 for 1 hour): ",
            "prompt_on_new": True,
        },
    )
    opportunity_refresh_interval: int = Field(
        default=60,
        json_schema_extra={
            "prompt": "How often to refresh opportunity data in seconds: ",
            "prompt_on_new": True,
        },
    )
    premium_tier_priority_slots: int = Field(
        default=2,
        json_schema_extra={
            "prompt": "Reserved slots for premium tier opportunities: ",
            "prompt_on_new": True,
        },
    )

    # Risk management
    profitability_to_take_profit: Decimal = Field(
        default=Decimal("0.01"),
        json_schema_extra={
            "prompt": "Take profit threshold (e.g. 0.01 for 1%): ",
            "prompt_on_new": True,
        },
    )
    funding_rate_diff_stop_loss: Decimal = Field(
        default=Decimal("-0.005"),
        json_schema_extra={
            "prompt": "Stop loss on funding rate differential: ",
            "prompt_on_new": True,
        },
    )
    position_time_limit_hours: int = Field(
        default=24,
        json_schema_extra={
            "prompt": "Maximum position hold time in hours: ",
            "prompt_on_new": True,
        },
    )

    # Reconciliation safety
    enable_reconciliation: bool = Field(default=True)
    reconciliation_interval: int = Field(default=5)
    exposure_warning_time: int = Field(default=10)
    max_exposure_time: int = Field(default=30)
    emergency_cancel_unfilled: bool = Field(default=True)
    emergency_use_market_orders: bool = Field(default=True)

    # Order execution configuration
    open_order_type: OrderType = Field(
        default=OrderType.LIMIT,
        json_schema_extra={
            "prompt": "Order type for opening positions (MARKET/LIMIT/LIMIT_MAKER): ",
            "prompt_on_new": True,
        },
    )
    close_order_type: OrderType = Field(
        default=OrderType.MARKET,
        json_schema_extra={
            "prompt": "Order type for closing positions (MARKET/LIMIT/LIMIT_MAKER): ",
            "prompt_on_new": True,
        },
    )

    @field_validator("open_order_type", "close_order_type", mode="before")
    @classmethod
    def parse_order_type(cls, v):
        """Convert string values to OrderType enum"""
        if isinstance(v, OrderType):
            return v

        if isinstance(v, str):
            # Convert to uppercase for case-insensitive matching
            lookup_key = v.upper()

            if lookup_key in ORDER_TYPE_MAP:
                return ORDER_TYPE_MAP[lookup_key]

            # Raise clear error with valid options
            valid_options = list(ORDER_TYPE_MAP.keys())
            raise ValueError(
                f"Invalid order type: '{v}'. "
                f"Valid options are: {', '.join(valid_options)}",
            )

        # If not a string or OrderType, raise error
        raise ValueError(f"Order type must be a string, got {type(v).__name__}")

    # Quote currency mapping for each exchange
    quote_currency_map: dict[str, str] = Field(
        default={
            "backpack_perpetual": "USDC",
            "binance_perpetual": "USDT",
            "hyperliquid_perpetual": "USD",
        },
        json_schema_extra={
            "prompt": "Quote currency for each exchange (e.g. {'binance_perpetual': 'USDT'}): ",
            "prompt_on_new": False,
        },
    )

    # Leverage
    leverage: int = Field(
        default=10,
        json_schema_extra={
            "prompt": "Leverage to use: ",
            "prompt_on_new": True,
        },
    )


class FundingArbitrageController(ControllerBase):
    """
    Controller for managing multiple funding arbitrage positions across tokens and exchanges.

    Responsibilities:
    - Scan all token-exchange pairs for funding opportunities
    - Rank opportunities by expected profitability
    - Allocate capital intelligently across best opportunities
    - Enforce global risk limits and exposure controls
    - Coordinate reconciliation and emergency hedging
    """

    def __init__(self, config: FundingArbitrageControllerConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config: FundingArbitrageControllerConfig = config

        # Track active opportunities with history
        self._active_opportunities: dict[str, FundingOpportunity] = {}
        self._opportunity_history: dict[str, FundingOpportunity] = {}  # Track all discovered opportunities
        self._allocated_capital: Decimal = Decimal(0)
        self._last_scan_time: float = 0
        self._scan_interval: float = config.opportunity_refresh_interval
        self._premium_slots_used: int = 0  # Track premium tier slot usage

    def get_trading_pair(self, token: str, connector: str) -> str:
        """Get the correct trading pair format for a token on a specific exchange"""
        quote_currency = self.config.quote_currency_map.get(connector, "USDT")
        return f"{token}-{quote_currency}"

    async def update_processed_data(self):
        """Scan market for funding arbitrage opportunities"""
        current_time = self.market_data_provider.time()

        elapsed = current_time - self._last_scan_time
        self.logger().debug(
            f"📊 update_processed_data called - elapsed: {elapsed:.1f}s, interval: {self._scan_interval}s",
        )

        # Only scan periodically to avoid excessive API calls
        if elapsed < self._scan_interval:
            self.logger().debug(f"⏳ Skipping scan, only {elapsed:.1f}s elapsed (need {self._scan_interval}s)")
            return

        self.logger().info(f"\n🚀 STARTING FUNDING OPPORTUNITY SCAN at {current_time:.1f}")
        self._last_scan_time = current_time

        # Scan all token-exchange combinations for opportunities
        fresh_opportunities = await self._scan_funding_opportunities()

        # Update opportunity history and classify tiers
        opportunities = self._update_opportunity_history(fresh_opportunities)

        # Remove stale opportunities
        opportunities = self._filter_stale_opportunities(opportunities)

        # Rank opportunities by priority score (considers tier and age)
        ranked_opportunities = sorted(
            opportunities,
            key=lambda x: x.priority_score,
            reverse=True,
        )

        # Store processed opportunities with tier information
        self.processed_data["opportunities"] = ranked_opportunities
        self.processed_data["available_capital"] = self._calculate_available_capital()
        self.processed_data["premium_opportunities"] = [
            o for o in ranked_opportunities if o.tier == OpportunityTier.PREMIUM
        ]
        self.processed_data["opportunity_tiers"] = self._count_opportunities_by_tier(ranked_opportunities)

    async def _scan_funding_opportunities(self) -> list[FundingOpportunity]:
        """Scan all token-exchange pairs for funding rate arbitrage opportunities"""
        self.logger().info(
            f"🔍 SCANNING OPPORTUNITIES - Tokens: {self.config.tokens}, "
            f"Connectors: {self.config.connectors}",
        )
        self.logger().info(f"📊 Min spread threshold: {self.config.min_funding_rate_profitability:.4%}")
        opportunities = []

        for token in self.config.tokens:
            # Get funding rates from all exchanges for this token
            self.logger().info(f"\n📈 Checking funding rates for {token}:")
            funding_rates = {}
            for connector in self.config.connectors:
                try:
                    # Get funding rate for token on this exchange
                    # Use exchange-specific quote currency
                    quote_currency = self.config.quote_currency_map.get(connector, "USDT")
                    symbol = f"{token}-{quote_currency}"
                    self.logger().debug(f"  Fetching funding info for {symbol} on {connector}")

                    funding_info = self.market_data_provider.get_funding_info(
                        connector_name=connector,
                        trading_pair=symbol,
                    )
                    if funding_info:
                        funding_rates[connector] = funding_info.rate
                        self.logger().info(
                            f"  ✅ {connector}: {symbol} funding rate = "
                            f"{funding_info.rate:.6%} ({funding_info.rate * 100:.4f}%)",
                        )
                    else:
                        self.logger().warning(f"  ❌ No funding info returned for {symbol} on {connector}")
                except Exception as e:
                    self.logger().error(f"  ❌ Failed to get funding for {token} on {connector}: {e}")

            # Find best long/short combination
            self.logger().info(f"  Found {len(funding_rates)} exchanges with funding data for {token}")
            if len(funding_rates) >= 2:
                self.logger().info(f"  📊 Funding rates collected: {funding_rates}")
                opportunity = self._find_best_opportunity(token, funding_rates)
                if opportunity:
                    self.logger().info(
                        f"  💡 Best opportunity - Spread: {opportunity.spread:.6%}, "
                        f"Long: {opportunity.long_exchange} ({opportunity.long_funding_rate:.6%}), "
                        f"Short: {opportunity.short_exchange} ({opportunity.short_funding_rate:.6%})",
                    )
                    if opportunity.spread >= self.config.min_funding_rate_profitability:
                        self.logger().info(
                            f"  🎯 OPPORTUNITY FOUND: {token} spread {opportunity.spread:.6%} >= "
                            f"threshold {self.config.min_funding_rate_profitability:.6%}",
                        )
                        opportunities.append(opportunity)
                    else:
                        self.logger().info(
                            f"  ❌ Spread {opportunity.spread:.6%} < "
                            f"threshold {self.config.min_funding_rate_profitability:.6%}, skipping",
                        )
                else:
                    self.logger().info(f"  No profitable opportunity found for {token}")
            else:
                self.logger().warning(f"  ⚠️ Need at least 2 exchanges with funding data, only got {len(funding_rates)}")

        self.logger().info(f"\n📊 SCAN COMPLETE - Found {len(opportunities)} viable opportunities")
        return opportunities

    def _find_best_opportunity(self, token: str, funding_rates: dict[str, Decimal]) -> FundingOpportunity | None:
        """Find the best long/short exchange combination for a token"""
        best_opportunity = None
        best_spread = Decimal(0)

        self.logger().debug(f"    Finding best opportunity from rates: {funding_rates}")

        exchanges = list(funding_rates.keys())
        for i, long_exchange in enumerate(exchanges):
            for j, short_exchange in enumerate(exchanges):
                if i == j:  # Skip same exchange
                    continue

                # Calculate spread (short funding - long funding)
                # Positive spread means we receive from short and pay on long
                spread = funding_rates[short_exchange] - funding_rates[long_exchange]

                self.logger().debug(
                    f"    Pair: Long {long_exchange} ({funding_rates[long_exchange]:.6%}) / "
                    f"Short {short_exchange} ({funding_rates[short_exchange]:.6%}) = Spread {spread:.6%}",
                )

                if spread > best_spread:
                    best_spread = spread
                    current_time = time.time()
                    tier = self._classify_opportunity_tier(spread)
                    best_opportunity = FundingOpportunity(
                        token=token,
                        long_exchange=long_exchange,
                        short_exchange=short_exchange,
                        long_funding_rate=funding_rates[long_exchange],
                        short_funding_rate=funding_rates[short_exchange],
                        spread=spread,
                        expected_profit=spread * self.config.position_size_quote,
                        required_capital=self.config.position_size_quote * 2,  # Capital for both legs
                        discovered_at=current_time,
                        last_updated=current_time,
                        tier=tier,
                    )
                    self.logger().debug(
                        f"    New best opportunity: spread={spread:.6%}, "
                        f"tier={tier.name if tier else 'None'}",
                    )

        return best_opportunity

    def _classify_opportunity_tier(self, spread: Decimal) -> OpportunityTier:
        """Classify opportunity into quality tiers based on spread"""
        if spread >= self.config.premium_tier_threshold:
            return OpportunityTier.PREMIUM
        elif spread >= self.config.standard_tier_threshold:
            return OpportunityTier.STANDARD
        else:
            return OpportunityTier.MARGINAL

    def _update_opportunity_history(self, fresh_opportunities: list[FundingOpportunity]) -> list[FundingOpportunity]:
        """Update opportunity history with fresh data and preserve discovery times"""
        updated_opportunities = []
        current_time = time.time()

        for opp in fresh_opportunities:
            opp_id = opp.opportunity_id

            # Check if we've seen this opportunity before
            if opp_id in self._opportunity_history:
                # Update existing opportunity but preserve discovery time
                existing = self._opportunity_history[opp_id]
                opp.discovered_at = existing.discovered_at
                opp.last_updated = current_time

                # Check if tier has improved
                if opp.tier and existing.tier and opp.tier.value < existing.tier.value:
                    self.logger().info(
                        f"🎯 Opportunity {opp.token} upgraded from {existing.tier.name} to {opp.tier.name}! "
                        f"Spread: {opp.spread:.4%}",
                    )
            else:
                # New opportunity
                opp.discovered_at = current_time
                opp.last_updated = current_time

                if opp.tier == OpportunityTier.PREMIUM:
                    self.logger().info(
                        f"💎 PREMIUM opportunity discovered: {opp.token} "
                        f"({opp.long_exchange}/{opp.short_exchange}) "
                        f"Spread: {opp.spread:.4%}",
                    )

            # Update history
            self._opportunity_history[opp_id] = opp
            updated_opportunities.append(opp)

        return updated_opportunities

    def _filter_stale_opportunities(self, opportunities: list[FundingOpportunity]) -> list[FundingOpportunity]:
        """Remove opportunities that are too old or stale"""
        filtered = []
        max_age = self.config.max_opportunity_age_hours * 3600  # Convert hours to seconds

        for opp in opportunities:
            if opp.age_seconds > max_age:
                self.logger().debug(
                    f"Removing stale opportunity {opp.opportunity_id}: "
                    f"Age {opp.age_seconds:.0f}s > {max_age}s",
                )
                # Remove from history
                if opp.opportunity_id in self._opportunity_history:
                    del self._opportunity_history[opp.opportunity_id]
            else:
                filtered.append(opp)

        return filtered

    def _count_opportunities_by_tier(self, opportunities: list[FundingOpportunity]) -> dict:
        """Count opportunities by tier for reporting"""
        counts = {
            OpportunityTier.PREMIUM: 0,
            OpportunityTier.STANDARD: 0,
            OpportunityTier.MARGINAL: 0,
        }

        for opp in opportunities:
            if opp.tier:
                counts[opp.tier] += 1

        return counts

    def _prioritize_opportunities(
        self,
        all_opportunities: list[FundingOpportunity],
        premium_opportunities: list[FundingOpportunity],
        current_premium_count: int,
    ) -> list[FundingOpportunity]:
        """Create priority queue with reserved slots for premium opportunities"""
        prioritized = []

        # First, add premium opportunities up to reserved slots
        premium_slots_available = self.config.premium_tier_priority_slots - current_premium_count
        if premium_slots_available > 0:
            prioritized.extend(premium_opportunities[:premium_slots_available])

        # Then add remaining opportunities by priority score
        remaining = [o for o in all_opportunities if o not in prioritized]
        prioritized.extend(remaining)

        return prioritized

    def _make_room_for_premium(self, premium_opp: FundingOpportunity, actions: list[ExecutorAction]) -> bool:
        """Try to close marginal positions to make room for premium opportunity"""
        # Find marginal positions that could be closed
        marginal_executors = [
            e for e in self.executors_info
            if e.status == RunnableStatus.RUNNING
            and hasattr(e, "tier")
            and e.tier == OpportunityTier.MARGINAL
        ]

        if marginal_executors:
            # Close the worst marginal position
            worst = min(marginal_executors, key=lambda e: getattr(e, "net_pnl_pct", 0))
            actions.append(StopExecutorAction(
                controller_id=self.config.id,
                executor_id=worst.id,
            ))
            self.logger().info(
                f"Closing marginal position {worst.id} to make room for premium opportunity {premium_opp.token}",
            )
            return True

        return False

    def _calculate_available_capital(self) -> Decimal:
        """Calculate available capital for new positions"""
        # Get allocated capital from active executors
        allocated = Decimal(0)
        for executor_info in self.executors_info:
            if executor_info.status == RunnableStatus.RUNNING:
                allocated += self.config.position_size_quote * 2  # Both legs

        return self.config.max_total_exposure - allocated

    def determine_executor_actions(self) -> list[ExecutorAction]:
        """Determine which executors to create/stop based on opportunities and capital"""
        actions = []

        # Get current opportunities and available capital
        opportunities = self.processed_data.get("opportunities", [])
        available_capital = self.processed_data.get("available_capital", Decimal(0))
        premium_opportunities = self.processed_data.get("premium_opportunities", [])

        # Count active positions and premium slots
        active_executors = [e for e in self.executors_info if e.status == RunnableStatus.RUNNING]
        active_count = len(active_executors)

        # Count current premium positions
        current_premium_count = sum(
            1 for e in active_executors
            if hasattr(e, "tier") and e.tier == OpportunityTier.PREMIUM
        )

        # Check if we should close any positions (poor performance or better opportunities available)
        actions.extend(self._check_positions_to_close(active_executors, opportunities))

        # Priority queue: First fill premium slots, then others
        opportunities_to_process = self._prioritize_opportunities(
            opportunities,
            premium_opportunities,
            current_premium_count,
        )

        # Create new executors for best opportunities within capital limits
        if active_count < self.config.max_positions:
            for opportunity in opportunities_to_process:
                # Check if we already have this position
                if self._is_opportunity_active(opportunity):
                    continue

                # Check capital availability
                if opportunity.required_capital > available_capital:
                    if opportunity.tier != OpportunityTier.PREMIUM:
                        break  # No more capital for non-premium positions
                    # For premium, try to make room by closing marginal positions
                    if not self._make_room_for_premium(opportunity, actions):
                        continue

                # Create executor for this opportunity
                action = self._create_executor_action(opportunity)
                if action:
                    actions.append(action)
                    available_capital -= opportunity.required_capital
                    active_count += 1

                    if opportunity.tier == OpportunityTier.PREMIUM:
                        self._premium_slots_used += 1

                    # Check position limit
                    if active_count >= self.config.max_positions:
                        break

        return actions

    def _is_opportunity_active(self, opportunity: FundingOpportunity) -> bool:
        """Check if we already have an active executor for this opportunity"""
        for executor_info in self.executors_info:
            if executor_info.status == RunnableStatus.RUNNING:
                config = executor_info.config
                if (config.token == opportunity.token and
                    config.long_connector_name == opportunity.long_exchange and
                        config.short_connector_name == opportunity.short_exchange):
                    return True
        return False

    def _check_positions_to_close(self, active_executors, new_opportunities) -> list[ExecutorAction]:
        """Check if any positions should be closed to make room for better opportunities"""
        actions = []

        # If we're at max positions and have better opportunities, close worst performers
        if len(active_executors) >= self.config.max_positions and new_opportunities:
            best_new_profit = new_opportunities[0].expected_profit if new_opportunities else Decimal(0)

            # Find worst performing executor
            worst_executor = None
            worst_performance = best_new_profit  # Only close if new opportunity is better

            for executor_info in active_executors:
                # Get executor's current performance
                if hasattr(executor_info, "net_pnl_pct"):
                    performance = executor_info.net_pnl_pct
                    if performance < worst_performance:
                        worst_performance = performance
                        worst_executor = executor_info

            # Close worst performer if found
            if worst_executor:
                actions.append(StopExecutorAction(
                    controller_id=self.config.id,
                    executor_id=worst_executor.id,
                ))

        return actions

    def _create_executor_action(self, opportunity: FundingOpportunity) -> CreateExecutorAction | None:
        """Create an executor action for a funding arbitrage opportunity"""
        try:
            # Create triple barrier config for risk management
            triple_barrier = TripleBarrierConfig(
                take_profit=self.config.profitability_to_take_profit,
                stop_loss=abs(self.config.funding_rate_diff_stop_loss),
                time_limit=self.config.position_time_limit_hours * 3600,  # Convert hours to seconds
            )

            # Get exchange-specific quote currencies
            long_quote = self.config.quote_currency_map.get(opportunity.long_exchange, "USDT")
            short_quote = self.config.quote_currency_map.get(opportunity.short_exchange, "USDT")

            # Create executor config
            executor_config = FundingArbitrageExecutorConfig(
                controller_id=self.config.id,
                timestamp=self.market_data_provider.time(),
                token=opportunity.token,
                long_connector_name=opportunity.long_exchange,
                short_connector_name=opportunity.short_exchange,
                long_trading_pair=f"{opportunity.token}-{long_quote}",
                short_trading_pair=f"{opportunity.token}-{short_quote}",
                position_size_quote=self.config.position_size_quote,
                leverage=self.config.leverage,
                triple_barrier_config=triple_barrier,
                min_funding_rate_profitability=self.config.min_funding_rate_profitability,
                trade_profitability_condition_to_enter=self.config.trade_profitability_condition_to_enter,
                funding_rate_diff_stop_loss=self.config.funding_rate_diff_stop_loss,
                max_exposure_time=self.config.max_exposure_time,
                exposure_warning_time=self.config.exposure_warning_time,
                emergency_use_market_orders=self.config.emergency_use_market_orders,
                open_order_type=self.config.open_order_type,
                close_order_type=self.config.close_order_type,
            )

            return CreateExecutorAction(
                executor_config=executor_config,
                controller_id=self.config.id,
            )

        except Exception as e:
            self.logger().error(f"Failed to create executor for {opportunity.token}: {e}")
            return None

    def to_format_status(self) -> list[str]:
        """Format controller status for display"""
        lines = []

        # Portfolio overview
        active_executors = [e for e in self.executors_info if e.status == RunnableStatus.RUNNING]
        allocated_capital = len(active_executors) * self.config.position_size_quote * 2
        available_capital = self.config.max_total_exposure - allocated_capital

        lines.extend([
            "=== Funding Arbitrage Controller Status ===",
            f"Active Positions: {len(active_executors)}/{self.config.max_positions}",
            f"Allocated Capital: ${allocated_capital:.2f}",
            f"Available Capital: ${available_capital:.2f}",
            f"Total Exposure Limit: ${self.config.max_total_exposure:.2f}",
        ])

        # Opportunity tiers summary
        tier_counts = self.processed_data.get("opportunity_tiers", {})
        if tier_counts:
            lines.append("\n🎯 Opportunity Tiers:")
            for tier, count in tier_counts.items():
                if tier == OpportunityTier.PREMIUM:
                    tier_symbol = "💎"
                elif tier == OpportunityTier.STANDARD:
                    tier_symbol = "🔶"
                else:
                    tier_symbol = "⚪"
                lines.append(f"  {tier_symbol} {tier.name}: {count} opportunities")

        # Current opportunities with tier indicators
        opportunities = self.processed_data.get("opportunities", [])
        if opportunities:
            lines.append("\n=== Top Opportunities (by Priority Score) ===")
            for i, opp in enumerate(opportunities[:5], 1):
                # Show quote currencies for clarity
                long_quote = self.config.quote_currency_map.get(opp.long_exchange, "USDT")
                short_quote = self.config.quote_currency_map.get(opp.short_exchange, "USDT")
                tier_indicator = ""
                if opp.tier == OpportunityTier.PREMIUM:
                    tier_indicator = " 💎[PREMIUM]"
                elif opp.tier == OpportunityTier.STANDARD:
                    tier_indicator = " 🔶[STANDARD]"
                else:
                    tier_indicator = " ⚪[MARGINAL]"

                age_indicator = ""
                if opp.age_seconds > 1800:  # More than 30 minutes old
                    age_indicator = f" ⏰({opp.age_seconds / 60:.0f}m old)"

                lines.append(
                    f"{i}. {opp.token}: {opp.long_exchange}({long_quote}) long / "
                    f"{opp.short_exchange}({short_quote}) short{tier_indicator} | "
                    f"Spread: {opp.spread:.4%} | Priority: {opp.priority_score:.4f}{age_indicator}",
                )

        # Premium opportunities specifically
        premium_opps = self.processed_data.get("premium_opportunities", [])
        if premium_opps:
            lines.append(f"\n💎 Premium Opportunities: {len(premium_opps)} available")
            for opp in premium_opps[:3]:
                lines.extend([
                    f"  - {opp.token}: {opp.spread:.4%} spread | "
                    f"Age: {opp.age_seconds / 60:.1f}m"
                    for opp in premium_opps[:3]
                ])

        # Active positions performance with tier
        if active_executors:
            lines.append("\n=== Active Positions ===")
            total_pnl = Decimal(0)
            for executor in active_executors:
                config = executor.config
                pnl = getattr(executor, "net_pnl_quote", Decimal(0))
                total_pnl += pnl

                tier_tag = ""
                if hasattr(executor, "tier"):
                    if executor.tier == OpportunityTier.PREMIUM:
                        tier_tag = " 💎"
                    elif executor.tier == OpportunityTier.STANDARD:
                        tier_tag = " 🔶"
                    else:
                        tier_tag = " ⚪"

                lines.append(
                    f"- {config.token} ({config.long_connector_name}/{config.short_connector_name}){tier_tag}: "
                    f"PnL: ${pnl:.2f}",
                )
            lines.append(f"Total Portfolio PnL: ${total_pnl:.2f}")

        return lines
