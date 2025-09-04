"""
Funding Arbitrage Controller - Portfolio-level management for multi-token funding arbitrage
Manages capital allocation, opportunity ranking, and global risk across multiple funding arbitrage positions
"""
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from pydantic import Field, field_validator

from hummingbot.client.settings import AllConnectorSettings
from hummingbot.core.data_type.common import MarketDict, OrderType, PriceType
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

    def age_seconds(self, current_time: float) -> float:
        """How long this opportunity has existed"""
        return current_time - self.discovered_at if self.discovered_at else 0

    def staleness_seconds(self, current_time: float) -> float:
        """How long since this opportunity was last updated"""
        return current_time - self.last_updated if self.last_updated else 0

    def priority_score(self, current_time: float) -> Decimal:
        """Calculate priority score for opportunity ranking"""
        # Base score is the spread
        score = self.spread

        # Premium tier gets 3x multiplier, standard 1.5x
        if self.tier == OpportunityTier.PREMIUM:
            score *= Decimal(3)
        elif self.tier == OpportunityTier.STANDARD:
            score *= Decimal("1.5")

        # Decay score based on age (reduce by 10% per hour)
        age_seconds = self.age_seconds(current_time)
        age_hours = Decimal(str(age_seconds / 3600))
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
    opportunity_refresh_interval_minutes: float = Field(
        default=1.0,
        json_schema_extra={
            "prompt": "How often to refresh opportunity data in minutes: ",
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
    min_position_age_before_replace_hours: float = Field(
        default=0.5,
        json_schema_extra={
            "prompt": "Minimum position age in hours before considering replacement (e.g. 0.5 for 30 minutes): ",
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

    def update_markets(self, markets: MarketDict) -> MarketDict:
        """
        Update the markets dict automatically from controller configuration.
        This eliminates the need to manually configure markets in the script config.
        """
        # Build markets dict from tokens and connectors
        for connector in self.connectors:
            if connector not in markets:
                markets[connector] = set()

            # Get the quote currency for this connector - fail fast if not configured
            if connector not in self.quote_currency_map:
                raise ValueError(
                    f"Connector '{connector}' not found in quote_currency_map. "
                    f"Please add it to the config. Available: {list(self.quote_currency_map.keys())}",
                )
            quote_currency = self.quote_currency_map[connector]

            # Add all token pairs for this connector
            connector_markets = markets[connector]
            for token in self.tokens:
                trading_pair = f"{token}-{quote_currency}"
                # mypy incorrectly infers Set[Set[str]] instead of Set[str] for GroupedSetDict values
                connector_markets.add(trading_pair)  # type: ignore[arg-type]

        return markets


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
        self._scan_interval: float = config.opportunity_refresh_interval_minutes * 60  # Convert minutes to seconds
        self._premium_slots_used: int = 0  # Track premium tier slot usage

    def on_stop(self):
        """Clean up controller state when stopped"""
        self.logger().info("🛑 Stopping FundingArbitrageController - cleaning up state")

        # Clear all tracking dictionaries
        self._active_opportunities.clear()
        self._opportunity_history.clear()

        # Reset state variables
        self._allocated_capital = Decimal(0)
        self._last_scan_time = 0
        self._premium_slots_used = 0

        # Clear processed data
        self.processed_data = {}

        self.logger().info("✅ Controller state cleaned up")

    def get_trading_pair(self, token: str, connector: str) -> str:
        """Get the correct trading pair format for a token on a specific exchange"""
        if connector not in self.config.quote_currency_map:
            raise ValueError(
                f"Connector '{connector}' not found in quote_currency_map. "
                f"Please add it to the config. Available: {list(self.config.quote_currency_map.keys())}",
            )
        quote_currency = self.config.quote_currency_map[connector]
        return f"{token}-{quote_currency}"

    async def update_processed_data(self):
        """Scan market for funding arbitrage opportunities"""
        current_time = self.market_data_provider.time()
        elapsed = current_time - self._last_scan_time

        # Only scan periodically to avoid excessive API calls
        # This respects the framework's synchronization - we only run when executors_update_event is set
        if elapsed < self._scan_interval:
            # Don't process opportunities yet - this prevents creating duplicate executors
            # The framework will call us again when executors are updated
            self.processed_data["opportunities"] = []
            self.processed_data["available_capital"] = Decimal(0)
            self.processed_data["premium_opportunities"] = []
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
        current_time = self.market_data_provider.time()
        ranked_opportunities = sorted(
            opportunities,
            key=lambda x: x.priority_score(current_time),
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
                    # Use exchange-specific quote currency - fail fast if not configured
                    if connector not in self.config.quote_currency_map:
                        self.logger().error(
                            f"Connector '{connector}' not in quote_currency_map. Skipping.",
                        )
                        continue
                    quote_currency = self.config.quote_currency_map[connector]
                    symbol = f"{token}-{quote_currency}"

                    # Check if the trading pair exists on this exchange by trying to get funding info
                    # Note: No has_market method, so we'll catch exceptions when fetching funding

                    self.logger().info(f"  Fetching funding info for {symbol} on {connector}")

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
                    # Calculate net spread for display (expected_profit / position_size)
                    net_spread = opportunity.expected_profit / self.config.position_size_quote
                    self.logger().info(
                        f"  💡 Best opportunity - Raw: {opportunity.spread:.6%} ({opportunity.spread * 100:.4f}%), "
                        f"Net: {net_spread:.6%} ({net_spread * 100:.4f}%), "
                        f"Profit: ${opportunity.expected_profit:.4f}",
                    )
                    self.logger().info(
                        f"     Long: {opportunity.long_exchange} @ {opportunity.long_funding_rate:.6%} "
                        f"({opportunity.long_funding_rate * 100:.4f}%), "
                        f"Short: {opportunity.short_exchange} @ {opportunity.short_funding_rate:.6%} "
                        f"({opportunity.short_funding_rate * 100:.4f}%)",
                    )
                    if net_spread >= self.config.min_funding_rate_profitability:
                        self.logger().info(
                            f"  🎯 OPPORTUNITY FOUND: {token} net spread {net_spread:.6%} ({net_spread * 100:.4f}%) >= "
                            f"threshold {self.config.min_funding_rate_profitability:.6%} "
                            f"({self.config.min_funding_rate_profitability * 100:.4f}%)",
                        )
                        opportunities.append(opportunity)
                    else:
                        self.logger().info(
                            f"  ❌ Net spread {net_spread:.6%} ({net_spread * 100:.4f}%) < "
                            f"threshold {self.config.min_funding_rate_profitability:.6%} "
                            f"({self.config.min_funding_rate_profitability * 100:.4f}%), skipping",
                        )
                else:
                    self.logger().info(f"  No profitable opportunity found for {token}")
            else:
                self.logger().warning(f"  ⚠️ Need at least 2 exchanges with funding data, only got {len(funding_rates)}")

        self.logger().info(f"\n📊 SCAN COMPLETE - Found {len(opportunities)} viable opportunities")
        return opportunities

    def _estimate_trading_fees(self, exchange: str, token: str) -> Decimal:
        """Estimate trading fees for an exchange based on configured order types

        Returns total fee percentage for round-trip (open + close)
        Accounts for probability of using market orders during reconciliation
        Fails fast if exchange not configured - no fallbacks
        """
        # Get exchange settings from config
        connector_settings = AllConnectorSettings.get_connector_settings()
        if exchange not in connector_settings:
            raise ValueError(
                f"Exchange {exchange} not configured in connector settings. "
                f"Cannot estimate fees for unconfigured exchange.",
            )

        fee_schema = connector_settings[exchange].trade_fee_schema
        if not fee_schema:
            raise ValueError(
                f"No fee schema configured for {exchange}. "
                f"Cannot trade without fee configuration.",
            )

        # Get maker and taker fees from schema - fail if not configured
        if fee_schema.maker_percent_fee_decimal is None:
            raise ValueError(f"Maker fee not configured for {exchange}")
        if fee_schema.taker_percent_fee_decimal is None:
            raise ValueError(f"Taker fee not configured for {exchange}")

        maker_fee = fee_schema.maker_percent_fee_decimal
        taker_fee = fee_schema.taker_percent_fee_decimal

        # Calculate expected fees based on configured order types
        # Open orders
        open_fee = maker_fee if self.config.open_order_type == OrderType.LIMIT else taker_fee

        # Close orders - account for possible escalation to market orders
        if self.config.close_order_type == OrderType.LIMIT:
            # Even with limit orders configured, there's a chance we escalate to market
            # during reconciliation or emergency closes
            # Estimate 70% limit (maker), 30% market (taker) for closes
            reconciliation_probability = Decimal("0.3")
            close_fee = (maker_fee * (1 - reconciliation_probability) +
                         taker_fee * reconciliation_probability)
        else:
            # Market orders configured for close
            close_fee = taker_fee

        # Round trip = open + close
        total_fee = open_fee + close_fee

        self.logger().info(
            f"Fee estimate for {token} on {exchange}: "
            f"open={open_fee:.5%}, close={close_fee:.5%}, total={total_fee:.5%}",
        )

        return total_fee

    def _find_best_opportunity(self, token: str, funding_rates: dict[str, Decimal]) -> FundingOpportunity | None:
        """Find the best long/short exchange combination for a token"""
        best_opportunity = None
        best_net_spread = Decimal(0)

        self.logger().info(f"    Finding best opportunity from rates: {funding_rates}")

        exchanges = list(funding_rates.keys())
        for i, long_exchange in enumerate(exchanges):
            for j, short_exchange in enumerate(exchanges):
                if i == j:  # Skip same exchange
                    continue

                # Calculate raw spread (short funding - long funding)
                # Positive spread means we receive from short and pay on long
                raw_spread = funding_rates[short_exchange] - funding_rates[long_exchange]

                # Estimate fees for both legs - skip if fees not configured
                try:
                    long_fees = self._estimate_trading_fees(long_exchange, token)
                    short_fees = self._estimate_trading_fees(short_exchange, token)
                    total_fees = long_fees + short_fees
                except ValueError as e:
                    self.logger().warning(
                        f"    Cannot estimate fees for {token} {long_exchange}/{short_exchange}: {e}. "
                        f"Skipping this pair.",
                    )
                    continue

                # Calculate net spread after fees
                net_spread = raw_spread - total_fees

                self.logger().info(
                    f"    Pair: Long {long_exchange} ({funding_rates[long_exchange]:.6%}) / "
                    f"Short {short_exchange} ({funding_rates[short_exchange]:.6%}) = "
                    f"Raw spread {raw_spread:.6%} ({raw_spread * 100:.4f}%), "
                    f"Fees {total_fees:.6%} ({total_fees * 100:.4f}%), "
                    f"Net {net_spread:.6%} ({net_spread * 100:.4f}%)",
                )

                if net_spread > best_net_spread:
                    best_net_spread = net_spread
                    current_time = self.market_data_provider.time()
                    tier = self._classify_opportunity_tier(net_spread)  # Tier based on net spread
                    best_opportunity = FundingOpportunity(
                        token=token,
                        long_exchange=long_exchange,
                        short_exchange=short_exchange,
                        long_funding_rate=funding_rates[long_exchange],
                        short_funding_rate=funding_rates[short_exchange],
                        spread=raw_spread,  # Keep raw spread for display
                        expected_profit=net_spread * self.config.position_size_quote,  # Net profit after fees
                        required_capital=self.config.position_size_quote * 2,  # Capital for both legs
                        discovered_at=current_time,
                        last_updated=current_time,
                        tier=tier,
                    )
                    self.logger().info(
                        f"    New best opportunity: net_spread={net_spread:.6%} ({net_spread * 100:.4f}%), "
                        f"expected_profit={net_spread * self.config.position_size_quote:.4f} quote, "
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
        current_time = self.market_data_provider.time()

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
        current_time = self.market_data_provider.time()

        for opp in opportunities:
            age_seconds = opp.age_seconds(current_time)
            if age_seconds > max_age:
                self.logger().info(
                    f"Removing stale opportunity {opp.opportunity_id}: "
                    f"Age {age_seconds:.0f}s > {max_age}s",
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
        # Note: ExecutorInfo doesn't have tier, but we can check the custom_info
        marginal_executors = [
            e for e in self.executors_info
            if (e.status == RunnableStatus.RUNNING and
                e.custom_info.get("tier") == OpportunityTier.MARGINAL)
        ]

        if marginal_executors:
            # Close the worst marginal position based on PnL
            worst = min(marginal_executors, key=lambda e: e.net_pnl_pct)
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
            if e.custom_info.get("tier") == OpportunityTier.PREMIUM.value
        )

        # Filter out opportunities for tokens we already have positions in
        # Type-safe access since we know these are FundingArbitrageExecutors
        active_tokens = set()
        for executor in active_executors:
            if executor.type == "funding_arbitrage_executor":
                # Safe to access token since we know the config type
                active_tokens.add(executor.config.token)
        new_opportunities = [opp for opp in opportunities if opp.token not in active_tokens]

        # Check if we should close any positions (poor performance or better opportunities available)
        # Only consider truly NEW opportunities, not ones we already have
        actions.extend(self._check_positions_to_close(active_executors, new_opportunities))

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
        # Check existing executors - look at all non-terminated statuses
        # The framework's synchronization (executors_update_event) ensures we won't
        # create duplicates during the async window
        active_executors = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda x: (
                x.status not in [RunnableStatus.TERMINATED, RunnableStatus.SHUTTING_DOWN] and
                x.type == "funding_arbitrage_executor"
            ),
        )

        for executor_info in active_executors:
            # Type-safe access to token attribute
            config = executor_info.config  # type: FundingArbitrageExecutorConfig
            if config.token == opportunity.token:
                return True

        return False

    def _check_positions_to_close(self, active_executors, new_opportunities) -> list[ExecutorAction]:
        """Check if any positions should be closed to make room for better opportunities"""
        actions = []

        # If we're at max positions and have better opportunities, close worst performers
        if len(active_executors) >= self.config.max_positions and new_opportunities:
            best_new_profit = new_opportunities[0].expected_profit if new_opportunities else Decimal(0)

            # Find worst performing executor that has been running long enough to evaluate
            # Don't close positions that just started (give them time to develop)
            current_time = self.market_data_provider.time()
            min_position_age_seconds = self.config.min_position_age_before_replace_hours * 3600  # Convert hours to seconds

            worst_executor = None
            worst_performance = best_new_profit  # Only close if new opportunity is better

            for executor_info in active_executors:
                # Skip recently created executors
                position_age = current_time - executor_info.timestamp
                if position_age < min_position_age_seconds:
                    continue

                # Get executor's current performance - ExecutorInfo has net_pnl_pct as a standard field
                performance = executor_info.net_pnl_pct
                if performance < worst_performance:
                    worst_performance = performance
                    worst_executor = executor_info

            # Close worst performer if found
            if worst_executor:
                self.logger().info(
                    f"Closing {worst_executor.custom_info.get('token', 'unknown')} executor "
                    f"(PnL: {worst_performance:.2%}) to make room for better opportunity "
                    f"(expected: {best_new_profit:.2%})",
                )
                actions.append(StopExecutorAction(
                    controller_id=self.config.id,
                    executor_id=worst_executor.id,
                ))

        return actions

    def _validate_and_adjust_position_size(
        self,
        token: str,
        position_size_quote: Decimal,
        long_connector: str,
        long_trading_pair: str,
        short_connector: str,
        short_trading_pair: str,
    ) -> Decimal | None:
        """Validate position size meets minimum order requirements for both exchanges.

        Returns position size if valid, or None if requirements cannot be met.
        """
        try:
            # Get current prices for conversion
            long_price = self.market_data_provider.get_price_by_type(
                long_connector, long_trading_pair, PriceType.MidPrice,
            )
            short_price = self.market_data_provider.get_price_by_type(
                short_connector, short_trading_pair, PriceType.MidPrice,
            )

            if not long_price or not short_price:
                self.logger().warning(f"Cannot get prices for {token} to validate position size")
                return None

            # Get trading rules for both exchanges
            long_rules = self.market_data_provider.get_trading_rules(long_connector, long_trading_pair)
            short_rules = self.market_data_provider.get_trading_rules(short_connector, short_trading_pair)

            # Check minimum order size requirements
            min_long_size = long_rules.min_order_size
            min_short_size = short_rules.min_order_size

            # Also check minimum notional requirements
            min_long_notional = long_rules.min_notional_size
            min_short_notional = short_rules.min_notional_size

            # Calculate minimum required position size in quote
            min_required_from_base = max(
                min_long_size * long_price,
                min_short_size * short_price,
            )
            min_required_from_notional = max(min_long_notional, min_short_notional)
            min_required = max(min_required_from_base, min_required_from_notional)

            # Add a small buffer (1%) to avoid edge cases
            min_required_with_buffer = min_required * Decimal("1.01")

            if position_size_quote < min_required_with_buffer:
                self.logger().info(
                    f"⚠️ Skipping {token}: Position size ${position_size_quote:.2f} below minimum "
                    f"${min_required_with_buffer:.2f} (long: {min_long_size:.8f} {token} / "
                    f"${min_long_notional:.2f}, short: {min_short_size:.8f} {token} / "
                    f"${min_short_notional:.2f})",
                )
                # Don't trade this token in this session - position size too small
                return None

            return position_size_quote

        except Exception as e:
            self.logger().error(f"Error validating position size for {token}: {e}")
            # In case of error, return original size and let executor handle it
            return position_size_quote

    def _create_executor_action(self, opportunity: FundingOpportunity) -> CreateExecutorAction | None:
        """Create an executor action for a funding arbitrage opportunity"""
        try:
            # Get exchange-specific quote currencies - these should always exist
            # since opportunities are only created for configured exchanges
            long_quote = self.config.quote_currency_map[opportunity.long_exchange]
            short_quote = self.config.quote_currency_map[opportunity.short_exchange]

            long_trading_pair = f"{opportunity.token}-{long_quote}"
            short_trading_pair = f"{opportunity.token}-{short_quote}"

            # Validate position size meets minimum order requirements
            valid_size = self._validate_and_adjust_position_size(
                opportunity.token,
                self.config.position_size_quote,
                opportunity.long_exchange,
                long_trading_pair,
                opportunity.short_exchange,
                short_trading_pair,
            )

            if valid_size is None:
                # Already logged in validation method
                return None

            # Create triple barrier config for risk management
            triple_barrier = TripleBarrierConfig(
                take_profit=self.config.profitability_to_take_profit,
                stop_loss=abs(self.config.funding_rate_diff_stop_loss),
                time_limit=self.config.position_time_limit_hours * 3600,  # Convert hours to seconds
                # Set order types from controller config
                open_order_type=self.config.open_order_type,
                take_profit_order_type=self.config.close_order_type,
                stop_loss_order_type=self.config.close_order_type,
                time_limit_order_type=self.config.close_order_type,
            )

            # Create executor config with adjusted position size
            executor_config = FundingArbitrageExecutorConfig(
                controller_id=self.config.id,
                timestamp=self.market_data_provider.time(),
                token=opportunity.token,
                long_connector_name=opportunity.long_exchange,
                short_connector_name=opportunity.short_exchange,
                long_trading_pair=long_trading_pair,
                short_trading_pair=short_trading_pair,
                position_size_quote=valid_size,
                leverage=self.config.leverage,
                triple_barrier_config=triple_barrier,
                min_funding_rate_profitability=self.config.min_funding_rate_profitability,
                trade_profitability_condition_to_enter=self.config.trade_profitability_condition_to_enter,
                funding_rate_diff_stop_loss=self.config.funding_rate_diff_stop_loss,
                max_unhedged_exposure_time=self.config.max_exposure_time,
                warning_exposure_time=self.config.exposure_warning_time,
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
                long_quote = self.config.quote_currency_map[opp.long_exchange]
                short_quote = self.config.quote_currency_map[opp.short_exchange]
                tier_indicator = ""
                if opp.tier == OpportunityTier.PREMIUM:
                    tier_indicator = " 💎[PREMIUM]"
                elif opp.tier == OpportunityTier.STANDARD:
                    tier_indicator = " 🔶[STANDARD]"
                else:
                    tier_indicator = " ⚪[MARGINAL]"

                age_indicator = ""
                age_seconds = opp.age_seconds(self.market_data_provider.time())
                if age_seconds > 1800:  # More than 30 minutes old
                    age_indicator = f" ⏰({age_seconds / 60:.0f}m old)"

                priority_score = opp.priority_score(self.market_data_provider.time())
                lines.append(
                    f"{i}. {opp.token}: {opp.long_exchange}({long_quote}) long / "
                    f"{opp.short_exchange}({short_quote}) short{tier_indicator} | "
                    f"Spread: {opp.spread:.4%} | Priority: {priority_score:.4f}{age_indicator}",
                )

        # Premium opportunities specifically
        premium_opps = self.processed_data.get("premium_opportunities", [])
        if premium_opps:
            lines.append(f"\n💎 Premium Opportunities: {len(premium_opps)} available")
            current_time = self.market_data_provider.time()
            lines.extend([
                f"  - {opp.token}: {opp.spread:.4%} spread | "
                f"Age: {opp.age_seconds(current_time) / 60:.1f}m"
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
                tier_value = executor.custom_info.get("tier")
                if tier_value == OpportunityTier.PREMIUM.value:
                    tier_tag = " 💎"
                elif tier_value == OpportunityTier.STANDARD.value:
                    tier_tag = " 🔶"
                elif tier_value == OpportunityTier.MARGINAL.value:
                    tier_tag = " ⚪"

                lines.append(
                    f"- {config.token} ({config.long_connector_name}/{config.short_connector_name}){tier_tag}: "
                    f"PnL: ${pnl:.2f}",
                )
            lines.append(f"Total Portfolio PnL: ${total_pnl:.2f}")

        return lines
